
import time
import sched
import concurrent.futures
import services.elastic as elastic
import services.rss as rss
import services.stock as stock
import services.doccano as doccano
import services.spacynlp as spacynlp
defInterval = 3600

class Service:
    
    def __init__(self, logging, config): 
        self.logging = logging 
        self.config = config.copy()
        self.index = elastic.Service(logging, config)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix='Scheduler')

    def statistics(self):
        return 'TODO'

    def scheduleProfileTasks(self, profile):
        # TODO validate config 
        #self.executor.max_workers += 1
        self.executor.submit(self._start, profile)

    def _start(self, profile):
        _sched = sched.scheduler(time.time, time.sleep) 
        indices = profile.get('indices')
        self.index.initIndices(indices)

        #self.index.addIndexFilter(indices.get('filters'), 'Jair Bolsonaro', '(jair messias) OR (jair bolsonaro) OR bonoro OR bolsonabo OR bolsomito')
        #self.index.addIndexFilter(indices.get('filters'), 'Covid-19', '(corona virus) OR covid OR covid19 OR cov19 OR (china virus)')


        self.schedule_index_tasks(_sched, indices, profile.get('indextasks'))
        self.schedule_doccano_tasks(_sched, indices, profile.get('projects'))

        return _sched.run()

    def schedule_index_tasks(self, scheduler, indices, tasks):
        for task in tasks:
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_index_task(scheduler, indices, task)
                else:
                    scheduler.enter(task.get('interval', defInterval), 3, self.run_index_task, (scheduler, indices, task)) 
    

    def schedule_project_tasks(self, scheduler, indices, tasks):
        for task in tasks:
            if task.get('enabled'):
                if task.get('startrun', False):
                    self.run_project_task(scheduler, indices, task)
                else:
                    scheduler.enter(task.get('interval', defInterval), 1, self.run_project_task, (scheduler, indices, task))


    def run_index_task(self, scheduler, indices, task):
        self.logging.info(f"Running scheduled task '{task.get('name')}'")
        try:
            # INDEX RSS FEEDS
            if task.get('type') == 'rss':
                rssService = rss.Service(self.logging, task, self.index)
                _result = rssService.index_feeds(indices, task.get('threads',0)) ## limiting number of feeds urls to the number of threads for fast testing
                #_result = rssService.index_feeds(indices)
            # INDEX STOCK SYMBOLS
            elif task.get('type') == 'stock':
                stockService = stock.Service(self.logging, task, self.index)
                exchangeCodes = task.get('exchanges', [])
                if len(exchangeCodes) == 0:
                    exchangeCodes = stockService.list_exchange_codes()
                stockService.index_stocks_symbols(exchangeCodes)
            # schedule next run
            self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
            scheduler.enter(task.get('interval', defInterval), 2, self.run_index_task, (scheduler, indices, task))
        except Exception as error:
                self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")

    def run_project_task(self, scheduler, indices, task):
        self.logging.info(f"Running scheduled task '{task.get('name')}'")
        try:
            # Idol <-> Doccano
            doccanoService = doccano.Service(self.logging, self.config, self.index)
            doccanoService.sync_idol_with_doccano(task)
            # Model training
            #spacyService = spacynlp.Service(self.logging, self.config, self.index)
            #spacyService.train_project_model(task)
            # schedule next run
            self.logging.info(f"Scheduling '{task.get('name')}' next run in {task.get('interval', defInterval)} seconds")
            scheduler.enter(task.get('interval', defInterval), 1, self.run_doccano_task, (scheduler, indices, task))
        except Exception as error:
            self.logging.error(f"error running task '{task.get('name')}' : {str(error)}")
