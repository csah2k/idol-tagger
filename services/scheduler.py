
import time
import sched
import concurrent.futures
import services.idol as idl
import services.rss as rss
import services.stock as stock
import services.doccano as doccano
import services.spacynlp as spacynlp
defInterval = 3600

class Service:
    
    scheduler = None
    executor = None
    config = None
    idol = None

    def __init__(self, logging, config): 
        self.logging = logging 
        self.config = config.copy()
        self.idol = idl.Service(logging, config)
        self.scheduler = sched.scheduler(time.time, time.sleep) 
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix='Scheduler')
        
    def statistics(self):
        return 'TODO'

    def start(self):
        self.executor.submit(self._start)
        #self._start()

    def _start(self):
        self.schedule_index_tasks()
        self.schedule_doccano_tasks()
        self.schedule_spacynlp_tasks()
        self.scheduler.run()

    def schedule_index_tasks(self):
        for task in self.config.get('indextasks'):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_index_task(task, self.scheduler)
                else:
                    self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                    self.scheduler.enter(task.get('interval', defInterval), 3, self.run_index_task, (task, self.scheduler)) 
    

    def schedule_doccano_tasks(self):
        for task in self.config.get('doccano',{}).get('projects', []):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_doccano_task(task, self.scheduler)
                else:
                    self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                    self.scheduler.enter(task.get('interval', defInterval), 1, self.run_doccano_task, (task, self.scheduler))

    def schedule_spacynlp_tasks(self):
        for task in self.config.get('spacynlp',{}).get('tasks', []):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_doccano_task(task, self.scheduler)
                else:
                    self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                    self.scheduler.enter(task.get('interval', defInterval), 2, self.run_spacynlp_task, (task, self.scheduler))

    def run_index_task(self, task, scheduler=None):
        self.logging.debug(f"Running scheduled task '{task.get('name')}'")
        try:
            # INDEX RSS FEEDS
            if task.get('type') == 'rss':
                rssService = rss.Service(self.logging, task, self.idol)
                _result = rssService.index_feeds(task.get('threads',0)) ## limiting number of feeds urls to the number of threads for fast testing
                #_result = rssService.index_feeds()
            # INDEX STOCK SYMBOLS
            elif task.get('type') == 'stock':
                stockService = stock.Service(self.logging, task, self.idol)
                exchangeCodes = task.get('exchanges', [])
                if len(exchangeCodes) == 0:
                    exchangeCodes = stockService.list_exchange_codes()
                stockService.index_stocks_symbols(exchangeCodes)
            # schedule next run
            if scheduler != None:
                self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 2, self.run_index_task, (task, self.scheduler))
        except Exception as error:
                self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")

    def run_doccano_task(self, task, scheduler=None):
        self.logging.debug(f"Running scheduled task '{task.get('name')}'")
        try:
            doccanoService = doccano.Service(self.logging, self.config, self.idol)
            # Idol <-> Doccano
            doccanoService.sync_idol_with_doccano(task)
            # schedule next run
            if scheduler != None:
                self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 1, self.run_doccano_task, (task, self.scheduler))
        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")

    def run_spacynlp_task(self, task, scheduler=None):
        self.logging.debug(f"Running scheduled task '{task.get('name')}'")
        try:
            spacyService = spacynlp.Service(self.logging, self.config, self.idol)
            # Idol <-> Doccano
            spacyService.train_model_classifier(task)
            # schedule next run
            if scheduler != None:
                self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 1, self.run_spacynlp_task, (task, self.scheduler))
        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")

   