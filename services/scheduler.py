
import time
import sched
import concurrent.futures
import services.rss as rss
import services.stock as stock
import services.spacynlp as spacynlp
import services.utils as util
from retrying import retry
from bson.objectid import ObjectId
from services.elastic import Service as elasticService
from services.doccano import Service as doccanoService

pooling_interval=3000

class Service:
    
    def __init__(self, logging, config, mongodb, doccano:doccanoService, index:elasticService): 
        maxtasks = config['service']['maxtasks']
        self.logging = logging 
        self.config = config
        self.doccano = doccano
        self.index = index 
        self.mongo_tasks = mongodb['tasks']
        self.mongo_users = mongodb['users']
        self.mongo_projects = mongodb['projects']
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=maxtasks+1, thread_name_prefix='Scheduler')
        self.logging.info(f"Scheduler service started  [{maxtasks} tasks]")

    def start(self):
        self.executor.submit(self.reset_tasks).result()
        self.executor.submit(self.handle_tasks)

    def statistics(self):
        return 'TODO'

    def reset_tasks(self):
        # set all tasks as NOT running
        curr_time = int(time.time())
        query = {"enabled":True, "running":True}
        self.mongo_tasks.update_many(query, {"$set":{"running":False}})

    @retry(wait_fixed=pooling_interval)
    def handle_tasks(self):
        try:
            curr_time = int(time.time())
            query = {"enabled": True, "running": False, "nextruntime": {"$lt": curr_time }}
            for task in self.mongo_tasks.find(query):                
                # add to executor pool 
                ## TODO SAVE THREAD ID IN MONGO, TO KILL/RESTART IT IF NEEDED
                self.executor.submit(self.runTask, task)
            self.mongo_tasks.update_many(query, {"$set":{"running":True, "lastruntime":curr_time}})
                
        except Exception as error:
            self.logging.error(f"Scheduler: {str(error)}") 
        raise Exception('sleeping')
            
    def runTask(self, task:dict):
        error = None
        start_time = time.time()
        try:
            # merge the user settings and the default config with current task
            username = util.getTaskUser(task)
            self.logging.info(f"Running task '{util.getTaskName(task)}' @ '{username}'")
            user = self.mongo_users.find_one({'username': username})
            task = util.merge_default_task_config(self, task)
            task.update({"user":user})

            # INDEX TASKS
            if task['type'] == 'rss':
                rssService = rss.Service(self.logging, task, self.index)
                _result = rssService.index_feeds()
            
            elif task['type'] == 'stock':
                stockService = stock.Service(self.logging, task, self.index)
                exchangeCodes = task.get('exchanges', [])
                if len(exchangeCodes) == 0:
                    exchangeCodes = stockService.list_exchange_codes()
                stockService.index_stocks_symbols(exchangeCodes)
            
            # DOCCANO
            elif task['type'] == 'doccano':
                self.doccano.sync_project_documents(task) 
                #spacyService = spacynlp.Service(self.logging, self.config, self.index)
                #spacyService.train_project_model(_task)

            elif task['type'] == 'sync_projects_users':
                self.doccano.sync_projects_users(task)

        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")
            error = str(error)
        finally:
            curr_time = time.time()
            elapsed_time = curr_time - start_time
            avgruntime = (task.get('avgruntime',elapsed_time)+elapsed_time)/2
            #cpu_usage = util.get_curr_thread_cpu_percent()  ## TODO
            #self.logging.warn(cpu_usage)
            update = {
                "running": False,
                "avgruntime": avgruntime,
                #"avgcpuusage": int((task('avgcpuusage',cpu_usage)+cpu_usage)/2),
                "nextruntime": int(curr_time+task.get('interval',60))
            }
            if error != None: update['error'] = error
            self.mongo_tasks.update_one({'_id': task['_id']}, {"$set": update})
