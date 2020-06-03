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
from copy import deepcopy
from bson import ObjectId
from requests.structures import CaseInsensitiveDict
from bson import Binary, Code
from bson.json_util import dumps

ALPHABET = string.ascii_letters + string.digits
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
    return task.get('name', f"{task.get('type','')}-{task.get('_id',task.get('id',''))}")

def getTaskUser(task:dict):
    return task.get('user',{}).get('username',task.get('username','anonymous'))


def filter_default_task_config(self, task:dict):
    _task = {}
    _params = {}
    for k,_v in self.tasks_defaults.get(task.get('type','default'),{}).get('params',{}).items():
        tv = task.get('params',{}).get(k, None)
        if tv != None: _params[k] = tv
    for k,_v in self.tasks_defaults.get(task.get('type','default'),{}).items():
        tv = task.get(k, None)
        if tv != None: _task[k] = tv
    if task.get('params', None) != None:
        _task['params'] = _params
    return _task

def merge_default_task_config(self, task:dict):
    _task = {}
    _params = {}
    for k,v in self.tasks_defaults.get(task.get('type','default'),{}).get('params',{}).items():
        _params[k] = task.get('params',{}).get(k, v)
    for k,v in self.tasks_defaults.get(task.get('type','default'),{}).items():
        _task[k] = task.get(k, v)
    if task.get('params', None) != None:
        _task['params'] = _params
    return _task

def getErrMsg(error):
    return {
        "error": str(error)
    }

def set_user_task(self, username:str, task:dict):
    ## try find task basic metadata for identification
    user = (self.mongo_users.find_one({"username": username}) or {})
    is_update = False
    task_basic = {
        "username": user.get('username', username)
    }
    if task.get('_id', None) != None: # if has "_id" then it is a update
        is_update = True
        task_basic['_id'] = ObjectId(str(task['_id']))
    if task.get('name', None) != None:
        task_basic['name'] = task['name']
    if task.get('type', None) in self.tasks_defaults.keys(): # filter invalid task types
        task_basic['type'] = task['type']
    
    # create mongo query based on the metadata
    query = task_basic.copy()
    if is_update: query.pop("name", None)
    else: query.pop("_id", None)

    # try find the original task
    updated_task = {}
    stored_task = self.mongo_tasks.find_one(query)
    if stored_task == None: # INSERT NEW TASK
        # default fields
        updated_task = merge_default_task_config(self, task)
        # stats fields
        updated_task.update({
            "nextruntime": int(time.time()) if task.get('startrun', updated_task.get('startrun', False)) else int(time.time())+task.get('interval', updated_task.get('interval', 60)),
            "lastruntime": task.get('lastruntime', updated_task.get('lastruntime', 0)),
            "avgruntime": task.get('avgruntime', updated_task.get('avgruntime', 0.0),),
            "startrun": task.get('startrun', updated_task.get('startrun', False)),
            "enabled": task.get('enabled', updated_task.get('enabled', False))
        })
        # novas tasks tem que ter um tipo
        if task_basic.get('type', None) == None:
            er = f"No valid task type supplied"
            self.logging.error(f"{er} @ '{username}'")
            return getErrMsg(er)
    else: ## UPDATE TASK
        updated_task = stored_task.copy()
        updated_task.update(task) ## TODO MERGE PARAMS
        updated_task['params'] = stored_task.get('params',{})
        updated_task['params'].update(task.get('params',{}))
        # update task - assure that basic and control fields remain untouched
        updated_task.pop('type', None)
        updated_task.pop('error', None)
        updated_task.pop('running', None)
        updated_task.pop('avgruntime', None)
        updated_task.pop('nextruntime', None)
        updated_task.pop('lastruntime', None)
        # stats fields
        if task.get('startrun', False):
            updated_task.update({
                "nextruntime": int(time.time()) if task.get('startrun', stored_task.get('startrun', False)) else int(time.time())+task.get('interval', stored_task.get('interval', 60)),
                "startrun": task.get('startrun', stored_task.get('startrun', False))
            })
    ### ========================================

    # check if user has access to the doccano project_id if any
    proj_id = updated_task.get('params',{}).get('projectid',None)
    system_username = self.doccano.login['username'] if hasattr(self, 'doccano') else self.login['username']
    if proj_id != None and system_username != task_basic['username'] : # bypass for doccano admin user
        if user.get('id', None) == None or self.mongo_projects.find_one({"id":proj_id, "users":{"$in":[user['id']]} }) == None :
            er = f"User '{username}' has No access in project {proj_id}"
            self.logging.warn(er)
            return getErrMsg(er)

    # update/insert in mongodb, dynamic triple key: ( _id, username, type) or ( name, username, type)
    updated_task.update(task_basic)
    self.mongo_tasks.update_one(query, {"$set": updated_task}, upsert=True)
    self.logging.info(f"Task updated {updated_task}")
    return updated_task

def getDataFilename(config, name, sufx=None, ext='dat', trunc=False, delt=False):
        datafile = None
        if ext != None: ext = f".{ext.strip()}"
        else: ext = ''
        if sufx != None: datafile = f"{name}_{sufx}{ext}"
        else: datafile = f"{name}{ext}"
        dataFolder = config.get('tempfolder', config.get('storage','data'))
        target_file = os.path.abspath(os.path.join(dataFolder, datafile))
        target_folder = os.path.dirname(target_file)
        os.makedirs(target_folder, exist_ok=True)
        if trunc: open(target_file, 'w').close()
        if delt and os.path.exists(target_file): os.remove(target_file)
        return target_file, target_folder, os.path.basename(target_file)

def createTrainDataQuery(proj, lang):
    return {
            "from" : 0, "size" : 1000,
            "query": {
                "bool" : {
                    "must" : {
                        "range" : {
                            proj['export_ts_field'] : {
                                "gte" : 10, ## HAVE TRAINING DATA
                            }
                        }
                    },
                    "filter": {
                        "term" : { "language": lang } # LANGUAGE MODEL
                    },
                    "must_not": {
                        "exists": {
                            "field": proj['train_ts_field'] ## NEW DATA ONLY
                        }
                    }
                }
            }
        }

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

def dict_of_dicts_merge(x, y):
    z = {}
    overlapping_keys = x.keys() & y.keys()
    for key in overlapping_keys:
        z[key] = dict_of_dicts_merge(x[key], y[key])
    for key in x.keys() - overlapping_keys:
        z[key] = deepcopy(x[key])
    for key in y.keys() - overlapping_keys:
        z[key] = deepcopy(y[key])
    return z
    
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)