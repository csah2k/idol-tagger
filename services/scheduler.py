
import time
import sched
import sqlite3
import concurrent.futures
import services.rss as rss
import services.stock as stock
import services.spacynlp as spacynlp
import services.utils as util
from retrying import retry
from bson.objectid import ObjectId
from services.elastic import Service as elasticService
from services.doccano import Service as doccanoService
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
        self.db = sqlite3.connect(':memory:', check_same_thread=False)
        self.db.cursor().execute("create table running_tasks (id, username, interval, lastruntime)")
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=maxtasks+1, thread_name_prefix='Scheduler')
        self.executor.submit(self.handle_tasks)
        self.logging.info(f"Scheduler service started: {maxtasks} maximum tasks")

    def statistics(self):
        return 'TODO'

    @retry(wait_fixed=10000)
    def handle_tasks(self):
        try:
            curr_time = int(time.time())
            cur = self.db.cursor().execute("select id, username from running_tasks where ( ? - lastruntime ) > interval", (curr_time, ))
            for row in cur.fetchall():
                self.logging.info(f"Running task '{row[0]}' @ '{row[1]}'")
                # get user & task updated metadata
                task = self.mongo_tasks.find_one({'_id': ObjectId(row[0])})
                profile = self.mongo_users.find_one({'username': row[1]})
                task.update(profile)
                # merge the user settings with the default config
                dft_cfg = self.config['tasks'][task.get('type')].copy()
                dft_cfg.update(task)
                # add to executor pool 
                self.executor.submit(self.runTask, task)
                # update lastruntime
                self.db.cursor().execute("update running_tasks set lastruntime = ? where id = ?", (curr_time, row[0]))
        except Exception as error:
            self.logging.error(f"Scheduler: {str(error)}") 
        raise Exception('sleeping')
            
    def runTask(self, task:dict):
        try:
            # INDEX RSS FEEDS
            if task['type'] == 'rss':
                rssService = rss.Service(self.logging, task, self.index)
                #_result = rssService.index_feeds(indices, task.get('threads',0)) ## limiting number of feeds urls to the number of threads for fast testing
                _result = rssService.index_feeds()
            # INDEX STOCK SYMBOLS
            elif task['type'] == 'stock':
                stockService = stock.Service(self.logging, task, self.index)
                exchangeCodes = task.get('exchanges', [])
                if len(exchangeCodes) == 0:
                    exchangeCodes = stockService.list_exchange_codes()
                stockService.index_stocks_symbols(exchangeCodes)
            # DOCCANO DATA SYNC
            elif task['type'] == 'doccano':
                self.doccano.sync_idol_with_doccano(task)
            # DOCCANO USERS/PROJECTS SYNC
            elif task['type'] == 'sync_doccano':
                self.doccano.sync_with_mongodb(task)
            # MODEL TRAINING
            elif task['type'] == 'spacynlp':
                spacyService = spacynlp.Service(self.logging, self.config, self.index)
                #spacyService.train_project_model(task)
        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")

    def schedule_all_users_tasks(self):
        self.executor.submit(self._schedule_all_users_tasks)

    def _schedule_all_users_tasks(self):
        for user in self.mongo_users.find():
            self._schedule_user_tasks(user)    

    def schedule_user_tasks(self, user:dict):
        self.executor.submit(self._schedule_user_tasks, user)

    def _schedule_user_tasks(self, user:dict):
        for task in self.mongo_tasks.find({"username": user['username']}):
            _task = self.config.get('user_tasks',{}).get(task.get('type'),{}).copy()
            _task.update({"user":user})
            _task.update(task)
            curr_time = 0 if _task.get('startrun',False) else int(time.time())
            self.logging.info(f"Scheduling task '{_task.get('name',_task['_id'])}' @ '{user['username']}' next run in {_task['interval']} seconds")
            self.db.cursor().execute("insert into running_tasks values (?, ?, ?, ?)", (str(_task['_id']), user['username'], _task['interval'], curr_time))            
            
