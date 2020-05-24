
import os
import json
import codecs
import logging
import datetime
import itertools
import operator
import concurrent.futures
from retrying import retry
from doccano_api_client import DoccanoClient
from requests.structures import CaseInsensitiveDict

from services.elastic import Service as elasticService
import services.utils as util

# https://github.com/doccano/doccano
# https://github.com/afparsons/doccano_api_client

class Service:

    def __init__(self, logging, config, index:elasticService): 
        self.logging = logging 
        self.config = config.get('doccano').copy()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 4), thread_name_prefix='DoccanoPool')
        self.index = index 
        login = self.config.get('login')
        self.doccano_login(self.config.get('url'), login.get('username'), login.get('password'))

    def doccano_login(self, url, username, password):
        return self.executor.submit(self._doccano_login, url, username, password).result()

    def _doccano_login(self, url, username, password):
        self.doccano_client = DoccanoClient(url, username, password)
        r_me = self.doccano_client.get_me().json()
        self.logging.debug(f"Doccano login: {r_me}")
        if not r_me.get('is_superuser'):
            self.logging.warn(f"User {username} is not a super-user!")
        return self.doccano_client

    def get_label_list(self, project_id):
        return self.executor.submit(self._get_label_list, project_id).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _get_label_list(self, project_id):
        return self.doccano_client.get_label_list(project_id).json()

    def populateProject(self, project):
        project_name = project.get('name', project.get('project')).strip()
        if project.get('name', None) == None: self.mergeProjectTask(project)
        projects_list = self.doccano_client.get_project_list().json()
        projects = [_p for _p in projects_list if _p.get('name') == project_name]
        if len(projects) == 0:
            self.logging.error(f"Doccano project '{project_name}' not found!")
            return None
        project.update(projects[0])
        self.logging.info(f"Doccano project: {project}")
        return project

    def mergeProjectTask(self, project):
        for _p in self.config.get('projects', []):
            if _p.get('name') == project.get('project'):
                prj = CaseInsensitiveDict(_p)   
                prj.pop('enabled', None)
                prj.pop('startrun', None)
                prj.pop('interval', None)
                project.update(prj)

    def sync_idol_with_doccano(self, project):
        return self.executor.submit(self._sync_idol_with_doccano, project).result()

    def _sync_idol_with_doccano(self, project):
        self.logging.info(f"==== Synchronizing Idol/Doccano ====> '{project.get('name')}'")
        _project = self.populateProject(project) # get project ID and other things
        if _project == None: return
        # IDOL => Doccano
        self.export_idol_to_doccano(_project)
        # Doccano => IDOL
        self.export_doccano_to_idol(_project)

    def export_idol_to_doccano(self, project):
        if self.export_training_from_idol(project) > 0:
            self.import_training_into_doccano(project)

    def export_training_from_idol(self, project):
        filepath, _, _ = util.getDataFilename(project['name'], 'export', 'tmp', True)
        docsToIndex = []
        docsToDelete = []
        with codecs.open(filepath, 'a', 'utf-8') as outfile:
            # check if doccano is full of pending docs to tag
            statistics = self.doccano_client.get_project_statistics(project.get('id')).json()
            maxremaining = project.get('maxremaining', 100)
            remaining = statistics.get('remaining', 0)
            if remaining >= maxremaining:
                self.logging.warn(f"Doccano project '{project['name']}' is already full [remaining: {remaining}, maxremaining: {maxremaining}]")
                return 0

            for _query in project.get('queries'):
                # get state token with the already traineds for filtering
                _stt_query = {
                    'DatabaseMatch': project.get('database'),
                    'AnyLanguage': True,
                    'MaxResults': 10000,
                    'Text':  _query.get('text', '*')
                }
                skippStateTkn = self.idol.get_statetoken(_stt_query)

                # update the idol query
                query = _query.copy()
                query.update({
                    'Print': 'All',
                    'StoredStateField': 'DREREFERENCE',
                    'StateDontMatchID': skippStateTkn
                })
                hits = self.idol.query(query) # query the documents
                self.logging.info(f"IDOL - hits: {len(hits)}, query: {query}")
                for hit in hits:
                    doc = hit.get('content',{}).get('DOCUMENT',[{}])[0]
                    text = doc.get(project.get('textfield'), [''])[0]
                    if len(text.strip()) > 10:
                        # CLASSIFICATION  ====>   {"text": "Great price.", "labels": ["positive"]}
                        # ENTITY EXTRACTION ==>   {"text": "President Obama", "labels": [ [10, 15, "PERSON"] ]}
                        labels = json.loads(doc.get(project.get('datafield'), ['[]'])[0])
                        hit['fields'] = [(project.get('datafield'), labels)]
                        meta = {
                            'url': util.getDocLink(doc),
                            'date': util.getDocDate(doc),
                            'links': ','.join(doc.get(f'{util.FIELDPREFIX_FILTER}_LNKS', [])),
                            'language': doc.get('LANGUAGE', util.DFLT_LANGUAGE+util.DFLT_ENCODE),
                            'reference': hit.get('reference')
                        }
                        jsonl = json.dumps({'text': text, 'labels': labels, 'meta': meta}, ensure_ascii=False).encode('utf8')
                        outfile.write(jsonl.decode()+'\n')
                        docsToDelete.append( (hit.get('database'), hit.get('reference')) )
                        docsToIndex.append(hit)
        # MOVE the selected docs to a staging database
        if len(docsToIndex) > 0: 
            query = {
                'DREDbName': project.get('database'),
                'KillDuplicates': 'REFERENCE', ## check for the same reference ONLY in 'DREDbName' database
                'CreateDatabase': True,
                'KeepExisting': True, ## do not replace content for matched references in KillDuplicates
                'Priority': 100
            }
            self.idol.index_into_idol(docsToIndex, query)

        if len(docsToDelete) > 0:
            it = itertools.groupby(docsToDelete, operator.itemgetter(0))
            for db, refsiter in it:
                self.idol.remove_documents([t[1] for t in refsiter], db)            

        return len(docsToIndex)
                
    def import_training_into_doccano(self, project):
        filepath, folderpath, filename = util.getDataFilename(project['name'], 'export', 'tmp')
        if not os.path.exists(filepath):
            self.logging.error(f"File not found: {filepath}")
            return
        resp = self.doccano_client.post_doc_upload(project.get('id'), 'json', filename, folderpath)
        if 200 <= resp.status_code < 300:
            self.logging.info(f"Uploaded to Doccano, file: '{filename}', code: {resp.status_code}")
            if os.path.exists(filepath): os.remove(filepath)
        else:
            self.logging.error(f"Erro uploading to Doccano, code: {resp.status_code}, file: {filepath}")
        return resp

    def export_doccano_to_idol(self, project):
        date = datetime.datetime.now().isoformat()
        resp = self.doccano_client.get_doc_download(project.get('id'), 'json')
        docsToIndex = []
        for line in resp.text.splitlines():
            self.logging.debug(line)
            doccanoDoc = json.loads(line)
            if doccanoDoc.get('annotation_approver', None) != None:            
                reference = doccanoDoc.get('meta',{}).get('reference')
                labels = json.dumps(doccanoDoc.get('annotations',[]), ensure_ascii=False).encode('utf8')
                text = doccanoDoc.get('text','')
                # get same doc from idol
                query = {
                    'DatabaseMatch': project.get('database'),
                    'Reference': reference
                }
                idolDoc = self.idol.get_content(query)
                self.logging.debug(idolDoc)
                if idolDoc != None:
                    self.logging.info(f"Document exported [doccano:{doccanoDoc.get('id')} -> idol:{idolDoc.get('id')}]") 
                    # update fields
                    idolDoc['content']['DOCUMENT'][0][project.get('textfield')] = [text]
                    idolDoc['content']['DOCUMENT'][0][project.get('datafield')] = [labels.decode()]                
                    idolDoc['content']['DOCUMENT'][0][project.get('datafield')+util.FIELDSUFFIX_TAGGED] = [date]
                    docsToIndex.append(idolDoc)
                    self.doccano_client.delete_document(project.get('id'), doccanoDoc.get('id'))
                else:
                    self.logging.warn(f"Can't find reference in idol [doccano:{doccanoDoc.get('id')} -> idol:{reference}]")
        if len(docsToIndex) > 0: 
            # index updated document
            query = {
                'DREDbName': project.get('database'),
                'KillDuplicates': 'REFERENCE',
                'CreateDatabase': True,
                'Priority': 100
            }
            self.idol.index_into_idol(docsToIndex, query)

