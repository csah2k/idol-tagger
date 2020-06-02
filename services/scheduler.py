
import time
import sched
import ctypes
import concurrent.futures
import services.rss as rss
import services.stock as stock
import services.spacynlp as spacynlp
import services.utils as util
from retrying import retry
from bson.objectid import ObjectId
from services.elastic import Service as elasticService
from services.doccano import Service as doccanoService
from services.spacynlp import Service as spacynlpService

pooling_interval_ms=10000
task_timeout_sec=3600

class Service:
    
    def __init__(self, logging, config, mongodb, doccano:doccanoService, index:elasticService, spacynlp:spacynlpService): 
        self.maxtasks = config['service']['maxtasks']
        self.running = False
        self.executing_tasks = {}
        self.logging = logging 
        self.config = config
        self.doccano = doccano
        self.index = index 
        self.spacynlp = spacynlp
        self.mongo_tasks = mongodb['tasks']
        self.mongo_users = mongodb['users']
        self.mongo_projects = mongodb['projects']
        self.tasks_defaults = config.get('tasks_defaults',{})
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.maxtasks+1, thread_name_prefix='Scheduler')
        self.running = self.executor.submit(self.initService).result()
    
    def initService(self):
        try:
            self.reset_tasks()
            self.executor.submit(self.handle_tasks)
            self.logging.info(f"Scheduler service started  [{self.maxtasks} tasks, mongodb: {self.mongo_tasks}]")
            return True
        except Exception as error:
            self.logging.error(f"Scheduler: {error}")
            return False


    def reset_tasks(self):
        query = {"running":True}
        self.mongo_tasks.update_many(query, {"$set":{"running":False}})

    @retry(wait_fixed=pooling_interval_ms)
    def handle_tasks(self):
        try:
            curr_time = int(time.time())
            # check for timeouted and done tasks to remove
            remove_tasks = []
            for task_id, task in self.executing_tasks.items():
                if task[1].done(): 
                    remove_tasks.append((task_id, task[1]))
                    self.mongo_tasks.update_one({'_id': ObjectId(task_id)}, {"$set":{"running":False}})
                if curr_time - task[0] > task_timeout_sec:
                    remove_tasks.append((task_id, task[1]))
                    self.logging.warn(f"Killing task by timeout, id: '{task_id}'") 
                    self.mongo_tasks.update_one({'_id': ObjectId(task_id)}, {"$set":{"running":False, "error":f"killed due to timeout after waiting {task_timeout_sec} secs"}})
            for task_id, thread in remove_tasks: 
                thread.cancel()
                self.executing_tasks.pop(task_id)

            for task in self.mongo_tasks.find({"enabled": True, "running": False, "nextruntime": {"$lt": curr_time }}):
                #self.logging.info(f"task found: {task}") 
                task_id = str(task['_id'])
                # check if tasks is already running
                is_running = False
                for _task_id, task in self.executing_tasks.items():       
                    if _task_id == task_id: is_running = True
                # add to executor pool 
                if is_running:                    
                    self.logging.warn(f"Task still running, id: '{task_id}'") 
                    self.mongo_tasks.update_one({'_id': ObjectId(task_id)}, {"$set":{"running":True, "error":"another instance is running"}})
                else: 
                    self.executing_tasks[task_id] = (curr_time, self.executor.submit(self.runTask, task))
                    self.mongo_tasks.update_one({'_id': ObjectId(task_id)}, {"$set":{"running":True, "lastruntime":curr_time}})
                
        except Exception as error:
            self.logging.error(f"Scheduler: {str(error)}") 
        raise Exception('sleeping')
            
    def runTask(self, task:dict):
        error = None
        start_time = time.time()
        try:
            # merge the user settings and the default config with current task
            username = util.getTaskUser(task)
            self.logging.info(f"Running task '{task.get('type','NO-TYPE')}' : '{util.getTaskName(task)}' @ '{username}'")
            user = self.mongo_users.find_one({'username': username})
            task.update({"user":user})

            # INDEX TASKS ## TODO ## DEVE SER UM MICROSERVIÇO ????
            if task['type'] == 'rss':  # enduser
                _rss = rss.Service(self.logging, task, self.index)
                _rss.index_feeds()
                #task_result = _rss.result()
            
            elif task['type'] == 'stock':  # enduser
                stockService = stock.Service(self.logging, task, self.index)
                exchangeCodes = task.get('exchanges', [])
                if len(exchangeCodes) == 0:
                    exchangeCodes = stockService.list_exchange_codes()
                stockService.index_stocks_symbols(exchangeCodes)
            
            elif task['type'] == 'import_from_index': # enduser
                self.doccano.import_from_index(task)
                        
            elif task['type'] == 'train_npl_models': # SYSTEM
                self.spacynlp.run_training_task(task)

            elif task['type'] == 'export_from_doccano': # SYSTEM
                self.doccano.export_from_doccano(task) 
                
            elif task['type'] == 'sync_doccano_metadada':  # SYSTEM
                self.doccano.sync_doccano_metadada()

        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")
            error = str(error)
        finally:
            curr_time = time.time()
            elapsed_time = curr_time - start_time
            avgruntime = (task.get('avgruntime',elapsed_time)+elapsed_time)*0.5
            update = {
                "running": False,
                "avgruntime": avgruntime,
                "nextruntime": int(curr_time+task.get('interval',60))
            }
            if error != None: update['error'] = error
            self.mongo_tasks.update_one({'_id': task['_id']}, {"$set": update})

