
import os
import json
import codecs
import logging
import datetime
import concurrent.futures
from doccano_api_client import DoccanoClient
from requests.structures import CaseInsensitiveDict
filters_fieldprefix = 'FILTERINDEX'
tagged_fieldsufix = '_TAGGED'

# https://github.com/doccano/doccano
# https://github.com/afparsons/doccano_api_client

class Service:

    doccano_client = None
    executor = None
    logging = None
    config = None
    idol = None

    def __init__(self, logging, config, idol): 
        self.logging = logging 
        self.config = config.get('doccano').copy()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2), thread_name_prefix='DoccanoPool')
        self.idol = idol 
        login = self.config.get('login')
        self.doccano_login(self.config.get('url'), login.get('username'), login.get('password'))

    def doccano_login(self, url, username, password):
        return self.executor.submit(self._doccano_login, url, username, password).result()

    def _doccano_login(self, url, username, password):
        self.doccano_client = DoccanoClient(url, username, password)
        r_me = self.doccano_client.get_me().json()
        self.logging.debug(f"Doccano login: {r_me}")
        if not r_me.get('is_superuser'):
            self.logging.warn(f"User username is not a super-user!")
        return self.doccano_client

    def populateProject(self, project):
        project_name = project.get('name').strip()
        projects_list = self.doccano_client.get_project_list().json()
        projects = [_p for _p in projects_list if _p.get('name') == project_name]
        self.logging.debug(f"all projects: {projects}")
        if len(projects) == 0:
            self.logging.error(f"Doccano project with name '{project_name}' not found!")
            return None
        project['id'] = projects[0].get('id')
        project['project_type'] = projects[0].get('project_type')
        self.logging.debug(f"Doccano project: '{project_name}', id: {project.get('id')}")
        return project

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
        filepath, _, _ = self.getDataFilename(project, 'export', 'tmp', True)
        docsToMove = []
        with codecs.open(filepath, 'a', 'utf-8') as outfile:
            # check if doccano is full of pending docs to tag
            statistics = self.doccano_client.get_project_statistics(project.get('id')).json()
            maxremaining = project.get('maxremaining', 100)
            remaining = statistics.get('remaining', 0)
            if remaining >= maxremaining:
                self.logging.warn(f"Doccano project '{project.get('name')}' is already full with documents to tag [remaining: {remaining}, maxremaining: {maxremaining}]")
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
                            'link': getDocLink(doc),
                            'date': getDocDate(doc),
                            'language': doc.get('LANGUAGE', ''),
                            'reference': hit.get('reference')
                        }
                        meta.update(getDocFilters(doc))
                        jsonl = json.dumps({'text': text, 'labels': labels, 'meta': meta}, ensure_ascii=False).encode('utf8')
                        outfile.write(jsonl.decode()+'\n')
                        docsToMove.append(hit)
        # MOVE the selected docs to a staging database
        if len(docsToMove) > 0: 
            query = {
                'DREDbName': project.get('database'),
                'KillDuplicates': 'REFERENCE', ## check for the same reference ONLY in 'DREDbName' database
                'CreateDatabase': True,
                'KeepExisting': True, ## do not replace content for matched references in KillDuplicates
                'Priority': 100
            }
            self.idol.index_into_idol(docsToMove, query)
        return len(docsToMove)
                
    def import_training_into_doccano(self, project):
        project = self.populateProject(project)
        filepath, folderpath, filename = self.getDataFilename(project, 'export', 'tmp')
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
        project = self.populateProject(project)
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
                    idolDoc['content']['DOCUMENT'][0][project.get('datafield')+tagged_fieldsufix] = [date]
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

    def getDataFilename(self, project, sufx=None, ext='dat', trunc=False, delt=False):
        datafile = None
        if sufx != None: datafile = f"{project.get('name')}_{sufx}.{ext}"
        else: datafile = f"{project.get('name')}.{ext}"
        dataFolder = self.config.get('tempfolder', 'data')
        target_file = os.path.abspath(os.path.join(dataFolder, datafile))
        target_folder = os.path.dirname(target_file)
        os.makedirs(target_folder, exist_ok=True)
        if trunc: open(target_file, 'w').close()
        if delt and os.path.exists(target_file): os.remove(target_file)
        return target_file, target_folder, os.path.basename(target_file)

## --------- helper functions ------------
def getDocLink(doc):
    return doc.get('URL', doc.get('LINK', doc.get('FEED', [''] )))[0]   

def getDocDate(doc):
    return doc.get('DATE', doc.get('DREDATE', doc.get('TIMESTAMP', [''] )))[0]   

def getDocFilters(doc):
    #references = doc.get(f'{filters_fieldprefix}_REFS', [])
    #dbname = doc.get(f'{filters_fieldprefix}_DBS', [])
    links = doc.get(f'{filters_fieldprefix}_LNKS', [])
    prefix = filters_fieldprefix.lower()
    return {
        #f'{prefix}_databases': ','.join(dbname),
        #f'{prefix}_references': ','.join(references),
        f'{prefix}_links': ','.join(links)
    }
