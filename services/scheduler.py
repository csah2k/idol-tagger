
import time
import sched
import sqlite3
import concurrent.futures
import services.elastic as elastic
import services.rss as rss
import services.stock as stock
import services.doccano as doccano
import services.spacynlp as spacynlp
import services.utils as util
from retrying import retry
from bson.objectid import ObjectId

class Service:
    
    def __init__(self, logging, config, mongodb): 
        maxtasks = config['service']['maxtasks']
        #dbfile = util.getDataFilename(config['service'], 'statistics', None, 'db')
        self.logging = logging 
        self.config = config
        self.users_profiles = mongodb['users_profiles']
        self.profiles_tasks = mongodb['profiles_tasks']
        self.index = elastic.Service(logging, config)
        self.db = sqlite3.connect(':memory:', check_same_thread=False)
        self.db.cursor().execute("create table running_tasks (id, login, interval, lastruntime)")
        #self.statistics = sqlite3.connect(dbfile, check_same_thread=False)
        #self.statistics.cursor().execute("create table tasks_executions (id, login, type, execution_time, elapsed_seconds, total_scanned, total_indexed)")
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=maxtasks+1, thread_name_prefix='Scheduler')
        self.executor.submit(self.handle_tasks)
        self.logging.info(f"Scheduler service started: {maxtasks} maximum tasks")

    def statistics(self):
        return 'TODO'

    @retry(wait_fixed=10000)
    def handle_tasks(self):
        try:
            curr_time = int(time.time())
            cur = self.db.cursor().execute("select id, login from running_tasks where ( ? - lastruntime ) > interval", (curr_time, ))
            for row in cur.fetchall():
                self.logging.info(f"Running task '{row[0]}' @ '{row[1]}'")
                # get user & task updated metadata
                task = self.profiles_tasks.find_one({'_id': ObjectId(row[0])})
                profile = self.users_profiles.find_one({'login': row[1]})
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
            # DOCCANO SYNC
            elif task['type'] == 'doccano':
                doccanoService = doccano.Service(self.logging, self.config, self.index)
                #doccanoService.sync_idol_with_doccano(task)
            # MODEL TRAINING
            elif task['type'] == 'spacynlp':
                spacyService = spacynlp.Service(self.logging, self.config, self.index)
                #spacyService.train_project_model(task)
        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")


    def scheduleAllProfilesTasks(self):
        self.executor.submit(self._scheduleAllProfilesTasks)

    def _scheduleAllProfilesTasks(self):
        for profile in self.users_profiles.find():
            self._scheduleProfileTasks(profile['login'])    

    def scheduleProfileTasks(self, login:str):
        self.executor.submit(self._scheduleProfileTasks, login)

    def _scheduleProfileTasks(self, login:str):
        profile = self.users_profiles.find_one({'login': login})
        for task in self.profiles_tasks.find({"login": login}):
            _task = self.config['tasks'][task.get('type')].copy()
            _task.update(profile)
            _task.update(task)
            curr_time = 0 if task.get('startrun',False) else int(time.time())
            self.logging.info(f"Scheduling '{_task.get('name')}' next run in {_task['interval']} seconds")
            self.db.cursor().execute("insert into running_tasks values (?, ?, ?, ?)", (str(_task['_id']), login, _task['interval'], curr_time))            
            
