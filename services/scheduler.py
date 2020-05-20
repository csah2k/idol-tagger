
import time
import sched
import concurrent.futures
defInterval = 3600

class Service:
    
    idolService = None
    executor = None
    doccano = None
    config = None
    stock = None
    rss = None

    def __init__(self, logging, config, idol, doccano, rss, stock): 
        self.logging = logging 
        self.config = config.copy()
        self.idolService = idol 
        self.doccano = doccano 
        self.stock = stock
        self.rss = rss
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix='Scheduler')
        
    def statistics(self):
        return 'TODO'

    def start(self):
        self.executor.submit(self._start).result()

    def _start(self):
        scheduler = sched.scheduler(time.time, time.sleep) 
        self.schedule_index_tasks(scheduler)
        self.schedule_doccano_tasks(scheduler)
        scheduler.run()
        return self.statistics()

    def schedule_doccano_tasks(self, scheduler):
        for task in self.config.get('doccano',{}).get('projects', []):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_doccano_task(task, None)
                # Start index tasks schedulers
                self.logging.debug(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 1, self.run_doccano_task, (task, scheduler))

    def schedule_index_tasks(self, scheduler):
        for task in self.config.get('indextasks'):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_index_task(task, None)
                # Start index tasks schedulers
                self.logging.debug(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 2, self.run_index_task, (task, scheduler))
    
    def run_doccano_task(self, task, scheduler):
        self.logging.debug(f"Running scheduled task '{task.get('name')}'")
        try:
            doccanoService = self.doccano.Service(self.logging, self.config, self.idolService)
            # Idol <-> Doccano
            doccanoService.sync_idol_with_doccano(task)
            # schedule next run
            if scheduler != None:
                self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 1, self.run_doccano_task, (task, scheduler))
        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")

    def run_index_task(self, task, scheduler):
        self.logging.debug(f"Running scheduled task '{task.get('name')}'")
        try:
            # INDEX RSS FEEDS
            if task.get('type') == 'rss':
                rssService = self.rss.Service(self.logging, task, self.idolService)
                #_result = rssService.index_feeds(task.get('threads',0)) ## limiting number of feeds urls to the number of threads for fast testing
                _result = rssService.index_feeds()
            # INDEX STOCK SYMBOLS
            elif task.get('type') == 'stock':
                stockService = self.stock.Service(self.logging, task, self.idolService)
                exchangeCodes = task.get('exchanges', [])
                if len(exchangeCodes) == 0:
                    exchangeCodes = stockService.list_exchange_codes()
                stockService.index_stocks_symbols(exchangeCodes)
            # schedule next run
            if scheduler != None:
                self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 2, self.run_index_task, (task, scheduler))
        except Exception as error:
                self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")