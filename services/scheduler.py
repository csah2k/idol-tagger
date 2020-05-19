
import time
import sched
import logging
from retrying import retry
import concurrent.futures
from requests.structures import CaseInsensitiveDict


defInterval = 3600

class Service:

    executor = None
    config = None
    idolService = None
    nlp = None
    rss = None
    stock = None
    scheduler = None

    def __init__(self, logging, config, idol, nlp, rss, stock): 
        self.logging = logging 
        self.config = config.copy()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix='Scheduler')
        self.idolService = idol 
        self.nlp = nlp 
        self.rss = rss
        self.stock = stock
            
    def statistics(self):
        return 'TODO'

    def start(self):
        self.executor.submit(self._start).result()

    def _start(self):
        scheduler = sched.scheduler(time.time, time.sleep) 
        self.schedule_index_tasks(scheduler)
        self.schedule_nlp_tasks(scheduler)
        scheduler.run()
        return self.statistics()

    def schedule_nlp_tasks(self, scheduler):
        for task in self.config.get('nlp',{}).get('projects', []):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_nlp_task(task, None)
                # Start index tasks schedulers
                self.logging.debug(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 1, self.run_nlp_task, (task, scheduler))

    def schedule_index_tasks(self, scheduler):
        for task in self.config.get('indextasks'):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_index_task(task, None)
                # Start index tasks schedulers
                self.logging.debug(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
                scheduler.enter(task.get('interval', defInterval), 2, self.run_index_task, (task, scheduler))
    
    def run_nlp_task(self, task, scheduler):
        self.logging.debug(f"Running scheduled task '{task.get('name')}'")
        nlpService = self.nlp.Service(self.logging, self.config, self.idolService)
        # IDOL => Doccano
        nlpService.export_idol_to_doccano(task)
        # Docano => IDOL
        nlpService.export_doccano_to_idol(task)
        # Train model
        nlpService.train_model_classifier(task)
        # schedule next run
        if scheduler != None:
            self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
            scheduler.enter(task.get('interval', defInterval), 1, self.run_nlp_task, (task, scheduler))

    def run_index_task(self, task, scheduler):
        self.logging.debug(f"Running scheduled task '{task.get('name')}'")
        # INDEX RSS FEEDS
        if task.get('type') == 'rss':
            rssService = self.rss.Service(self.logging, task, self.idolService)
            _result = rssService.index_feeds(task.get('threads',0)) ## limiting number of feeds urls to fast testing
            #_result = rssService.index_feeds()
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