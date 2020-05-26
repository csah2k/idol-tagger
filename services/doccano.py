
import os
import re
import json
import codecs
import hashlib 
import logging
import datetime
import itertools
import operator
import concurrent.futures
import urllib.parse
import django_admin_client
from retrying import retry
from doccano_api_client import DoccanoClient
from requests.structures import CaseInsensitiveDict
from services.elastic import Service as elasticService
import services.utils as util



# https://github.com/doccano/doccano
# https://github.com/afparsons/doccano_api_client

class Service:

    def __init__(self, logging, config, mongodb, index:elasticService): 
        self.logging = logging 
        self.config = config
        self.cfg = config['doccano'].copy()
        self.index = index 
        self.mongo_tasks = mongodb['tasks']
        self.mongo_users = mongodb['users']
        self.mongo_labels = mongodb['labels']
        self.mongo_projects = mongodb['projects']
        self.mongo_documents = mongodb['documents']
        self.mongo_document_annotations = mongodb['document_annotations']
        login = self.cfg['login']
        numthreads = self.cfg.get('threads',2)
        djangourl = urllib.parse.urljoin(self.cfg['url'], 'admin')
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=numthreads, thread_name_prefix='DoccanoPool')
        self.doccano_client = DoccanoClient(self.cfg['url'], login['username'], login['password']) ## TODO isso tem que sair, usar apenas o Django 
        self._django_client = django_admin_client.DjangoAdminBase(djangourl, login['username'], login['password'])
        self.django_client = django_admin_client.DjangoAdminDynamic(spec=self._django_client.generate_spec(), client=self._django_client)
        self.logging.info(f"Doccano service started: {login['username']} @ {djangourl} [{numthreads} threads]")
 


    def import_from_index(self, task:dict):
        # get project info
        proj_id = task['projectid']
        proj = self.mongo_projects.find_one({"id":proj_id})
        proj_type = proj['project_type'][0]
        self.logging.info(f"import_from_index -> proj_id: '{proj_id}', proj_type: '{proj_type}'")
        indices = task['user']['indices']
        self.logging.info(f"indices -> {indices}")
        for hit in self.index.query({ 'query': task['query'] }, indices['indexdata']):
            self.logging.info(f"index hit -> {hit}")




    def generate_training_data(self, task:dict):
        # get project info
        proj_id = task['projectid']
        proj = self.mongo_projects.find_one({"id":proj_id})
        proj_type = proj['project_type'][0]
        self.logging.info(f"generate_training_data -> proj_id: '{proj_id}', proj_type: '{proj_type}'")
        if proj_type == "DocumentClassification":
            self.generate_training_doc_class(proj)
        # TODO other types
    

    def generate_training_doc_class(self, project:dict):
        # get project labels
        proj_id = project['id']
        proj_labels = {}
        for label in self.mongo_labels.find({"project":[proj_id]}):
            proj_labels[label.get('id')] = label.get('text')
        self.logging.info(f"proj_labels: {proj_labels}")

        lookup = { 
            "from": "document_annotations",
            "localField": "id",
            "foreignField": "document",
            "as": "annotations"
        }
        self.logging.info(f"lookup: {lookup}")
        for doc in self.mongo_documents.aggregate([
                {"$lookup": lookup },
                {"$match": {"project":[proj_id], "annotations": {"$exists": True, "$ne": []}  } },
                #{'$sort': { sort: order } }
            ]):
            self.logging.info(f"doc: {doc}")
            # TODO output somewhere in the correct format
            


    def get_user(self, username:str):    
        return self.executor.submit(self._get_user, username).result()

    def _get_user(self, username:str):
        query = {"username": username}
        user = self.mongo_users.find_one(query)
        self.logging.info(f"get_user '{username}': {user}")
        return user

    def sync_annotations_with_mongodb(self):
        return self.executor.submit(self._sync_annotations_with_mongodb).result()

    def _sync_annotations_with_mongodb(self):
        # list doccano annotations
        threads = []
        for anot_id in self.django_client.document_annotations.all().get('ids',[]):
            threads.append(self.executor.submit(self._sync_annotation_to_mongo, anot_id))
        for _t in threads:
            _t.result()

    def _sync_annotation_to_mongo(self, anot_id:str):
        annotation = self.django_client.document_annotations.get(anot_id).get('details',{})
        if self.mongo_document_annotations.count_documents({"id":anot_id}) <= 0:
            annotation.update({
                "id": anot_id
            })
            self.mongo_document_annotations.insert_one(annotation)
            self.logging.info(f"Anotation added '{anot_id}'")
        else:
            self.mongo_document_annotations.update_one({"id":anot_id}, {"$set":annotation})
            self.logging.debug(f"Anotation updated '{anot_id}'")
        return annotation

    def sync_documents_with_mongodb(self):
        return self.executor.submit(self._sync_documents_with_mongodb).result()

    def _sync_documents_with_mongodb(self):
        # list doccano documents
        threads = []
        for doc_id in self.django_client.documents.all().get('ids',[]):
            threads.append(self.executor.submit(self._sync_doc_to_mongo, doc_id))
        for _t in threads:
            _t.result()

    def _sync_doc_to_mongo(self, doc_id:str):
        doc = self.django_client.documents.get(doc_id).get('details',{})
        if self.mongo_documents.count_documents({"id":doc_id}) <= 0:
            doc.update({
                "id": doc_id,
                "meta": json.loads(doc.get("meta","{}"))
            })
            self.mongo_documents.insert_one(doc)
            self.logging.info(f"Doc added '{doc_id}'")
        else:
            self.mongo_documents.update_one({"id":doc_id}, {"$set":doc})
            self.logging.debug(f"Doc updated '{doc_id}'")
        return doc

    def sync_projects_users_labels(self):
        threads = [
            self.executor.submit(self._sync_projects_with_mongodb), 
            self.executor.submit(self._sync_labels_with_mongodb),
            self.executor.submit(self._sync_users_with_mongodb),
            ]
        for _t in threads:
            _t.result()

    def _sync_labels_with_mongodb(self):
        # list doccano labels
        for lbl_id in self.django_client.labels.all().get('ids',[]):
            query = {"id": lbl_id}
            label = self.django_client.labels.get(lbl_id).get('details',{})
            label.update(query)
            if self.mongo_labels.count_documents(query) <= 0:
                self.mongo_labels.insert_one(label)
                self.logging.info(f"Label added '{label.get('id',lbl_id)}'")
            else:
                self.mongo_labels.update_one(query, {"$set": label})
                self.logging.debug(f"Label updated '{label.get('id',lbl_id)}'")

    def _sync_users_with_mongodb(self):
        # list doccano users
        res = self.django_client.users.all()
        for user_id in res['ids']:
            query = {"id": user_id}
            user = self.django_client.users.get(user_id).get('details',{})
            user.update(query)
            if self.mongo_users.count_documents(query) <= 0:
                # generate indexes names
                user.update({
                    "indices": {
                        "indexdata": str(hashlib.md5(("indexdata"+user.get('username',user_id)).encode("utf-8")).hexdigest()).lower(),
                        "filters": str(hashlib.md5(("filters"+user.get('username',user_id)).encode()).hexdigest()).lower(),
                    }
                })
                ## add user indicies in Elastic Server
                self.index.initIndices(user['indices'])
                ## add in mongodb
                self.mongo_users.insert_one(user)
                self.logging.info(f"User added '{user.get('username',user_id)}'")
            else:
                self.mongo_users.update_one(query, {"$set": user})
                self.logging.debug(f"User updated '{user.get('username',user_id)}'")

    def _sync_projects_with_mongodb(self):
        # list doccano projects
        res = self.django_client.projects.all()
        # flag for removal
        self.mongo_projects.update_many({}, {"$set":{"remove":True}})
        for prj_id in res['ids']:
            query = {"id": prj_id}
            project = self.django_client.projects.get(prj_id).get('details',{})
            project.update({"id":prj_id, "remove":False})
            if self.mongo_projects.count_documents(query) <= 0:
                ## add task in mongodb
                task = util.merge_default_task_config(self, {'type':'doccano', 'name':project['name'], 'projectid': prj_id})
                util.set_user_task(self, util.ADMIN_USERNAME, task)  
                ## add project in mongodb
                self.mongo_projects.insert_one(project)
                self.logging.info(f"Project added '{project.get('name',prj_id)}'")
            else:
                ## update task in mongodb
                task = util.merge_default_task_config(self, {'type':'doccano', 'name':project['name'], 'projectid': prj_id})
                util.set_user_task(self, util.ADMIN_USERNAME, task)  
                ## update project in mongodb
                self.mongo_projects.update_one(query, {"$set": project})
                self.logging.debug(f"Project updated '{project.get('name',prj_id)}'")
        # handle removed projects ans tasks
        remove_project_ids = [_p['id'] for _p in self.mongo_projects.find({"remove":True})]
        if len(remove_project_ids) > 0:
            query = { "username": util.ADMIN_USERNAME, "projectid": {"$in":remove_project_ids} }
            self.mongo_tasks.delete_many(query)
            self.mongo_projects.delete_many({"remove":True})

    def get_user_projects(self, user_id:str):
        return self.executor.submit(self._get_user_projects, user_id).result()

    def _get_user_projects(self, user_id:str):
        doccano_user_id = self.django_client.users.find(user_id)
        res = self.doccano_projects.find( { 'users': [user_id] } )
        self.logging.info(res)
        return res








    ### ============================ REMOVER ==================
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

