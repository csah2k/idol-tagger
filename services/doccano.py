# pylint: disable=no-member

import os
import re
import time
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
        self.running = False
        self.logging = logging 
        self.config = config
        self.tasks_defaults = config.get('tasks_defaults',{})
        self.cfg = config['doccano'].copy()
        self.index = index 
        self.mongo_tasks = mongodb['tasks']
        self.mongo_users = mongodb['users']
        self.mongo_roles = mongodb['roles']
        self.mongo_labels = mongodb['labels']
        self.mongo_projects = mongodb['projects']
        self.mongo_documents = mongodb['documents']
        self.mongo_role_mappings = mongodb['role_mappings']
        self.login = self.cfg['login']
        numthreads = self.cfg.get('threads',2)
        djangourl = urllib.parse.urljoin(self.cfg['url'], 'admin')
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=numthreads, thread_name_prefix='DoccanoPool')
        try:
            self.doccano_client = DoccanoClient(self.cfg['url'], self.login['username'], self.login['password'])
            self._django_client = django_admin_client.DjangoAdminBase(djangourl, self.login['username'], self.login['password'])
            self.django_client = django_admin_client.DjangoAdminDynamic(spec=self._django_client.generate_spec(), client=self._django_client)
            self.logging.info(f"Doccano service started [django: '{self.login['username']}@{djangourl}', threads: {numthreads}]")
            self.running = True
        except Exception as error:
            self.logging.error(f"cant connect to doccano api at '{self.login['username']}@{djangourl}' error -> {str(error)}")
    

    def get_label_list(self, project_id:int):
        return self.executor.submit(self._get_label_list, project_id).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _get_label_list(self, project_id:int):
        return self.doccano_client.get_label_list(project_id).json()

    ### INDEX -> DOCCANO
    def import_from_index(self, task:dict):
        return self.executor.submit(self._import_from_index, task).result()

    def _import_from_index(self, task:dict):
        # get project info
        proj_id = task['params']['projectid']
        indices = task['user']['indices']
        task_query = task['params'].get('query_text', None)
        ## assure required fields exists in the user index
        import_ts_field = f"import_ts_prj_{proj_id}"
        export_ts_field = f"export_ts_prj_{proj_id}"
        train_ts_field = f"train_ts_prj_{proj_id}"
        export_field = f"export_prj_{proj_id}"
        self.index.add_index_field(indices['indexdata'], import_ts_field, "long")
        self.index.add_index_field(indices['indexdata'], export_ts_field, "long")
        self.index.add_index_field(indices['indexdata'], train_ts_field, "long")
        self.index.add_index_field(indices['indexdata'], export_field, "object")
        
        # create the search query
        index_query ={
                "query":{
                    "bool": {
                        "must_not": {
                            "exists": {
                                "field": import_ts_field
                            }
                        }
                    }
                }
            }
        if task_query != None and len(str(task_query).strip()) > 1:
            index_query['query']['bool']['must'] = { 'match': { "content": str(task_query).strip() } }

        # check if doccano is full of pending docs to tag
        statistics = self.doccano_client.get_project_statistics(proj_id)
        maxremaining = task['params'].get('maxremaining', 100)
        remaining = statistics.get('remaining', 0)
        self.logging.info(f"Importing into project_{proj_id} {statistics}")
        if remaining >= maxremaining:
            self.logging.warn(f"Doccano project '{task['name']}' is full [remaining: {remaining}, maxremaining: {maxremaining}]")
            return 0
        imported = 0
        for hit in self.index.query(index_query, indices['indexdata']):
            text = hit.get('content', hit.get('title',  hit.get('text', hit.get('summary',''))))
            if len(text) <3: continue # <3 :)
            meta = {
                "date": hit.get('date',''),
                "url": hit.get('url', hit.get('link', '')),
                "task": hit.get('indextask',hit.get('task_id','')),
                "language": hit.get('language', hit.get('lang', 'unknow')),
                "index": hit.get('index',''),
                "id": hit['id']
            }
            # IMPORT VIA DOCCANO API
            res = self.doccano_client.create_document(proj_id, text, json.dumps(meta))
            if (res or {}).get('id', 0) > 0:
                # ADD METADATA IN DOCUMENT TO AVOID IMPORT SAME DOC MULTIPLE TIMES
                _res = self.index.update_fields(indices['indexdata'], hit['id'], { import_ts_field : int(time.time()) } )
                if (_res or {}).get('result',None) == 'updated':
                    imported += 1
                else:
                    self.logging.error(f"Error updating index field -> {_res}")
        self.logging.info(f"Total imported into project_{proj_id}: {imported}, query {index_query}")
        return imported

    ### DOCCANO -> INDEX
    def export_from_doccano(self, task:dict):
        return self.executor.submit(self._export_from_doccano, task).result()

    def _export_from_doccano(self, task:dict):
        # get project info
        proj_id = task['params']['projectid']
        export_ts_field = f"export_ts_prj_{proj_id}"
        export_field = f"export_prj_{proj_id}"
        export_count = 0
        resp = self.doccano_client.get_doc_download(int(proj_id), 'json')
        for line in resp.text.splitlines():
            doc = json.loads(line)            
            # only approved 
            if doc.get('annotation_approver',None) != None:
                doc.update({"projectid" : proj_id})
                meta = doc.get('meta',{})
                doc_id = meta.get('id',None)
                doc_indx = meta.get('index',None)                
                ## update a field in the elastic search
                _res = self.index.update_fields(doc_indx, doc_id, 
                    {
                        export_ts_field: int(time.time()),
                        export_field : doc.get('annotations',[]),
                        'content': doc.get('text')
                    })
                
                if (_res or {}).get('result',None) == 'updated':
                    res = self.doccano_client.delete_document(int(proj_id), doc.get('id'))
                    if 200 <= res.status_code < 300:
                        self.logging.info(f"Doc exported [doccano: {doc.get('id')} -> index: {doc_indx}, id: {doc_id}]")
                        export_count += 1
                    else:
                        self.logging.error(f"Erro deleting from Doccano, code: {res.status_code}, proj_id: {proj_id}, doc_id: {doc.get('id')}")
                else:
                    self.logging.error(f"Error updating index field -> {_res}")
        return export_count


    
    # ============ MAIN METADATA SYNC =========== #
    def sync_doccano_metadada(self):
        threads = [
            self.executor.submit(self._sync_role_mappings_with_mongodb),
            self.executor.submit(self._sync_projects_with_mongodb), 
            self.executor.submit(self._sync_labels_with_mongodb),
            self.executor.submit(self._sync_roles_with_mongodb),
            self.executor.submit(self._sync_users_with_mongodb),            
            self.executor.submit(self._assure_system_admin_rights)
            ]
        for _t in threads:
            _t.result()

    def _assure_system_admin_rights(self):
        ###### assure that the admin user has admin_right on all projects ######
        ## find admin user doccano id
        admin_userid = (self.mongo_users.find_one({"username":self.login['username']}) or {}).get('id',None)
        if admin_userid == None:
            self.logging.warn(f"Admin user not found: '{self.login['username']}'")
            return False
        ## find project_admin role id
        project_admin_roleid = (self.mongo_roles.find_one({"name": "project_admin"}) or {}).get('id',None)
        if project_admin_roleid == None:
            self.logging.warn(f"Admin Role 'project_admin' not found")
            return False
        # fix wrong roles
        wrong_roles = self.mongo_role_mappings.find({"user":[admin_userid], "role":{"$nin":[project_admin_roleid]}})
        for role in wrong_roles:
            _role  = {
                "user": [admin_userid],
                "project": role['project'],
                "role": [project_admin_roleid],
            }
            res = self.django_client.role_mappings.change(role['id'], _role)
            # handle django fucking html format
            if not res.get('created', False):
                self.logging.warn(f"django_client.role_mappings.change  {_role} -> error: {util.cleanDjangoError(res)}")
            else:
                self.logging.info(f"Admin role '{role['id']}' changed in project {role['project']}")

        ## get all projects id where the admin role is missing 
        projects_not_admin = self.mongo_projects.distinct('id',
            {
                "id": {"$nin":self.mongo_role_mappings.distinct('project',{"user":[admin_userid], "role":[project_admin_roleid]})}
            })
        if len(projects_not_admin) <= 0: return
        for proj_id in projects_not_admin:
            role  = {
                "user": [admin_userid],
                "project": [proj_id],
                "role": [project_admin_roleid],
            }
            # add role in doccano
            res = self.django_client.role_mappings.add(role)
            # handle django fucking html format
            if not res.get('created', False):
                self.logging.warn(f"django_client.role_mappings.add  {role} -> error: {util.cleanDjangoError(res)}")
            else:
                self.logging.info(f"Admin role granted to project '{proj_id}'")

   
    def _sync_roles_with_mongodb(self):
        # list all doccano roles
        for role_id in self.django_client.roles.all().get('ids',[]):
            query = {"id": role_id}
            role = self.django_client.roles.get(role_id).get('details',{})
            role.update(query)
            self.mongo_roles.update_one(query, {"$set": role}, upsert=True)
            self.logging.debug(f"Role updated '{role_id}'")
        
    def _sync_role_mappings_with_mongodb(self):
        # flag for removal
        _flag = self.mongo_role_mappings.update_many({}, {"$set":{"remove":True}})
        # list all doccano roles-mappings
        for role_id in self.django_client.role_mappings.all().get('ids',[]):
            query = {"id": role_id}
            role = self.django_client.role_mappings.get(role_id).get('details',{})
            role.update({"id":role_id, "remove":False})
            self.mongo_role_mappings.update_one(query, {"$set": role}, upsert=True)
            self.logging.debug(f"RoleMap updated '{role_id}'")

        removed = self.mongo_role_mappings.delete_many({"remove":True})
        if removed.deleted_count > 0:
            self.logging.info(f"RoleMaps removed: {removed.deleted_count}")

    def _sync_labels_with_mongodb(self):
        # list all doccano labels
        for lbl_id in self.django_client.labels.all().get('ids',[]):
            query = {"id": lbl_id}
            label = self.django_client.labels.get(lbl_id).get('details',{})
            label.update(query)
            self.mongo_labels.update_one(query, {"$set": label}, upsert=True)
            self.logging.debug(f"Label updated '{label.get('id',lbl_id)}'")
                

    def _sync_users_with_mongodb(self):
        # list all doccano users
        res = self.django_client.users.all()
        for user_id in res['ids']:
            query = {"id": user_id}
            user = self.django_client.users.get(user_id).get('details',{})
            user.update(query)
            # generate indexes names
            user.update({
                "indices": {
                    "indexdata": str(hashlib.md5(("indexdata"+user.get('username',user_id)).encode("utf-8")).hexdigest()).lower(),
                    "filters": str(hashlib.md5(("filters"+user.get('username',user_id)).encode()).hexdigest()).lower(),
                }
            })
            ## create user indicies in Elastic Server
            self.index.initIndices(user['indices'])
            ## add/update in mongodb
            self.mongo_users.update_one(query, {"$set": user}, upsert=True)
            self.logging.info(f"User updated '{user.get('username',user_id)}'")

    def _sync_projects_with_mongodb(self):
        # list all doccano projects
        res = self.django_client.projects.all()
        # flag for removal
        _flag = self.mongo_projects.update_many({}, {"$set":{"remove":True}})
        _flag = self.mongo_tasks.update_many({"username": self.login['username'], 'type':'export_from_doccano'}, {"$set":{"remove":True}})
        for prj_id in res['ids']:
            query = {"id": prj_id}
            project = self.django_client.projects.get(prj_id).get('details',{})

            ## add export task in mongodb (system)
            task = {'type':'export_from_doccano', 'name':project['name'], 'params': {'projectid': prj_id}, 'startrun': False, "remove":False}
            util.set_user_task(self, self.login['username'], task)

            ## update/create project in mongodb
            project.update({"id":prj_id, "remove":False})
            self.mongo_projects.update_one(query, {"$set": project}, upsert=True)
            self.logging.debug(f"Project updated '{project.get('name',prj_id)}'")
                
        # handle removed projects & tasks
        removed = self.mongo_tasks.delete_many({"remove":True})
        self.logging.info(f"{removed.deleted_count} Tasks removed")
        removed = self.mongo_projects.delete_many({"remove":True})
        self.logging.info(f"{removed.deleted_count} Projects removed")
    


