
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

    def __init__(self, logging, config, idol, nlp, rss, stock): 
        self.logging = logging 
        self.config = config
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 3), thread_name_prefix='Scheduler')
        self.idolService = idol 
        self.nlp = nlp 
        self.rss = rss
        self.stock = stock
        self.start()

    def statistics(self):
        return 'TODO'

    def start(self):
        self.executor.submit(self._start_index)
        self.executor.submit(self._start_nlp)

    @retry(wait_fixed=5000, stop_max_delay=defInterval)
    def _start_index(self):
        try:
            indexTasksSched = sched.scheduler(time.time, time.sleep) 
            self.schedule_index_tasks(indexTasksSched)
            indexTasksSched.run()
        except Exception as error:
            self.logging.error(f"start_index - {str(error)}")
            raise error

    @retry(wait_fixed=5000, stop_max_delay=defInterval)
    def _start_nlp(self):
        try:
            nlpTasksSched = sched.scheduler(time.time, time.sleep)
            self.schedule_nlp_tasks(nlpTasksSched)
            nlpTasksSched.run()
        except Exception as error:
            self.logging.error(f"start_nlp - {str(error)}")
            raise error
    
    def schedule_nlp_tasks(self, scheduler):
        for task in self.config.get('nlp',{}).get('projects', []):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_nlp_task(task, None)
                # Start index tasks schedulers
                self.logging.info(f"Scheduling nlp project task: '{task.get('name')}'")
                scheduler.enter(task.get('interval', defInterval), 1, self.run_nlp_task, (task, scheduler))
        
    def run_nlp_task(self, task, scheduler):
        self.logging.info(f"Running nlp project '{task.get('name')}' task ...")
        nlpService = self.nlp.Service(self.logging, self.config, self.idolService)
        # IDOL => Doccano
        nlpService.export_idol_to_doccano(task)
        # Docano => IDOL
        nlpService.export_doccano_to_idol(task)
        # Train model
        nlpService.train_model_classifier(task)
        # schedule next run
        if scheduler != None:
            scheduler.enter(task.get('interval', defInterval), 1, self.run_nlp_task, (task, scheduler))

    def schedule_index_tasks(self, scheduler):
        for task in self.config.get('indextasks'):
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_index_task(task, None)
                # Start index tasks schedulers
                self.logging.info(f"Scheduling index task: '{task.get('name')}'")
                scheduler.enter(task.get('interval', defInterval), 2, self.run_index_task, (task, scheduler))
        
    def run_index_task(self, task, scheduler):
        self.logging.info(f"Running index task '{task.get('name')}' ...")
        # INDEX RSS FEEDS
        if task.get('type') == 'rss':
            rssService = self.rss.Service(self.logging, task, self.idolService)
            results = rssService.index_feeds()
            for _r in results:
                self.logging.info(_r)
        # INDEX STOCK SYMBOLS
        elif task.get('type') == 'stock':
            stockService = self.stock.Service(self.logging, task, self.idolService)
            exchangeCodes = task.get('exchanges', [])
            if len(exchangeCodes) == 0:
                exchangeCodes = stockService.list_exchange_codes()
            stockService.index_stocks_symbols(exchangeCodes)
        # schedule next run
        if scheduler != None:
            scheduler.enter(task.get('interval', defInterval), 2, self.run_index_task, (task, scheduler))