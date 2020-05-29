import os
import re
import time
import json
import string
import logging
#import psutil
#import threading
import html
import html2text
from bson import ObjectId
from requests.structures import CaseInsensitiveDict
from bson import Binary, Code
from bson.json_util import dumps

ALPHABET = string.ascii_letters + string.digits
ADMIN_USERNAME = 'admin'
FIELDPREFIX_FILTER = 'FILTERINDEX'
FIELDSUFFIX_TAGGED = '_TAGGED'
FIELDSUFFIX_TRAINED = '_TRAINED'
DFLT_LANGUAGE = 'GENERAL'
DFLT_ENCODE = 'UTF8'

## --------- helper functions ------------
def makeUrl(component):
    return f"{component.get('protocol','http')}://{component.get('host','localhost')}:{component.get('port',9000)}"

def aciQuery(query, modify={}):
    _q = CaseInsensitiveDict(query)   
    _q.pop('a', None)
    _q.pop('action', None)
    _q.pop('actionid', None)
    _q.pop('responseformat', None)
    _q.update(modify)
    return _q

def hashDict(dct):
    return str(hash(frozenset(dct.items())))

def cleanText(text):
    text_maker = html2text.HTML2Text()
    text_maker.ignore_links = True
    text_maker.ignore_images = True
    text = html.unescape(text)
    text = text_maker.handle(text)
    text = text.strip().capitalize()
    return text

def cleanDjangoError(response):
    errors = [cleanText(str(e)) for e in (response or {}).get('errors',['error'])]
    if len(errors) > 0:
        return re.sub(r'[\s\W]+', ' ', '; '.join(errors))[:100].rsplit(' ', 1)[0]+'...'

def getDocLink(doc):
    return doc.get('URL', doc.get('LINK', doc.get('FEED', [''] )))[0]   

def getDocDate(doc):
    return doc.get('DATE', doc.get('DREDATE', doc.get('TIMESTAMP', [''] )))[0]   

def getTaskName(task:dict):
    return task.get('name', f"{task.get('type','<type?>')}-{task.get('_id',task.get('id',''))}")

def getTaskUser(task:dict):
    return task.get('user',{}).get('username',task.get('username','anonymous'))

def merge_default_task_config(self, task:dict):
    _task = self.tasks_defaults.get(task.get('type','default'),{}).copy()
    _params = _task.get('params',{}).copy()
    _params.update(task.get('params',{}))
    _task.update(task)
    _task['params'] = _params
    return _task

def getErrMsg(error):
    return {
        "error": str(error)
    }

def set_user_task(self, username:str, task:dict):
    query = None
    if task.get('_id', None) != None:
        query = {"username": username, '_id': ObjectId(task['_id'])}
    elif username == ADMIN_USERNAME: # id is not necessary for admin tasks
        query = {"username": username, "name":getTaskName(task)}
    if query != None and task.get('type', None) != None:
        query.update({ "type" : task.get('type') })

    # ==== safety checks ====
    # check valid task type
    if task.get('type', None) not in self.tasks_defaults.keys():
        er = f"Invalid task type: '{task.get('type', None)}'"
        self.logging.error(f"{er} @ '{username}'")
        return getErrMsg(er)

    # ==== other tasks validations ====
    if task.get('type',None) == "import_from_index":
        # check if user has access to projectid if any
        proj_id = task.get('params',{}).get('projectid',None)
        if proj_id != None: 
            user = self.mongo_users.find_one({"username": username})
            proj = self.mongo_projects.find_one({"id":proj_id, "users":[user['id']] })
            if proj == None :
                er = f"User {user['id']} project {proj_id} not found @ '{username}'"
                self.logging.error(er)
                return getErrMsg(er)

    ## TODO ??? white list of allowed key names in task object
    
    task.update({"username": username})
    task = merge_default_task_config(self, task)
    task.update({
        "nextruntime": int(time.time()) if task.get('startrun',False) else int(time.time())+task.get('interval',60),
        "lastruntime": task.get('lastruntime', 0),
        "avgruntime": task.get('avgruntime', 0.0),
        #"avgcpuusage": task.get('avgcpuusage', 0.0),
        "running": False,
        "error": None
    })
    _id = task.pop('_id', None)
    if query == None or self.mongo_tasks.count_documents(query)<=0:      
        # new user task   
        _id = str(self.mongo_tasks.insert_one(task).inserted_id)
        self.logging.info(f"Task added: '{_id}' @ '{username}'")
    else:
        # update task
        task.pop('type', None)
        task.pop('error', None)
        task.pop('running', None)
        self.mongo_tasks.update_one(query, {"$set": task})
        self.logging.info(f"Task updated '{_id or getTaskName(task)}' @ '{username}'")

    task['_id'] = _id
    return task

    

def getDocFilters(doc):
    #references = doc.get(f'{FIELDPREFIX_FILTER}_REFS', [])
    #dbname = doc.get(f'{FIELDPREFIX_FILTER}_DBS', [])
    links = doc.get(f'{FIELDPREFIX_FILTER}_LNKS', [])
    #prefix = FIELDPREFIX_FILTER.lower()
    return {
        #f'{prefix}_databases': ','.join(dbname),
        #f'{prefix}_references': ','.join(references),
        f'LINKS': ','.join(doc.get(f'{FIELDPREFIX_FILTER}_LNKS', []))
    }

def getProjectLastRuntime(project, db):
    table = db['executions']
    return table.find(order_by='-runtime', _limit=1)
    #for row in results:
    #    print(f"row['runtime']")

def getDataFilename(config, name, sufx=None, ext='dat', trunc=False, delt=False):
        datafile = None
        if sufx != None: datafile = f"{name}_{sufx}.{ext}"
        else: datafile = f"{name}.{ext}"
        dataFolder = config.get('tempfolder', config.get('storage','data'))
        target_file = os.path.abspath(os.path.join(dataFolder, datafile))
        target_folder = os.path.dirname(target_file)
        os.makedirs(target_folder, exist_ok=True)
        if trunc: open(target_file, 'w').close()
        if delt and os.path.exists(target_file): os.remove(target_file)
        return target_file, target_folder, os.path.basename(target_file)

def dump_json(dic:dict):
    return dumps(dic)

def getLogLvl(cfg):
    lvl = cfg.get('service',{}).get('loglevel', 'INFO').strip().upper()
    loglvl = logging.INFO if lvl == 'INFO' else None
    if loglvl == None: loglvl = logging.DEBUG if lvl == 'DEBUG' else None
    if loglvl == None: loglvl = logging.WARN if lvl == 'WARN' else None
    if loglvl == None: loglvl = logging.WARNING if lvl == 'WARNING' else None
    if loglvl == None: loglvl = logging.ERROR if lvl == 'ERROR' else None
    if loglvl == None: loglvl = logging.FATAL if lvl == 'FATAL' else None
    return loglvl

'''
def get_curr_thread_cpu_percent(interval=0.1):
    return get_threads_cpu_percent(threading.currentThread(), interval)

def get_threads_cpu_percent(p, interval=0.1):
   total_percent = p.cpu_percent(interval)
   total_time = sum(p.cpu_times())
   return [total_percent * ((t.system_time + t.user_time)/total_time) for t in p.threads()]
'''

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)