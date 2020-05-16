from __future__ import unicode_literals, print_function

import plac
import json
import random
import logging
import datetime
import sched, time
import concurrent.futures
import services.idol as idol
import services.stock as stock
import services.rss as rss
import services.nlp as nlp
from requests.structures import CaseInsensitiveDict

# TODO - add twitter source
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

defInterval = 3600
logfile='main.log'
config = {}
srvcfg = {}
#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
with open('config.json') as json_configfile:
    config = json.load(json_configfile)
    srvcfg = config.get('service',{})
logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=logging.INFO, handlers=[logging.FileHandler(srvcfg.get('logfile', logfile), 'w', 'utf-8')])
scheduler = sched.scheduler(time.time, time.sleep)
idolService = idol.Service(logging, config)

def main():
    logging.info("==============> Starting service... ")
    logging.info(srvcfg)

    schedule_index_tasks()
    schedule_nlp_tasks()

    scheduler.run()


def schedule_nlp_tasks():
    for project in config.get('nlp',{}).get('projects', []):
        if project.get('enabled'):
            if project.get('startrun', False):
                run_nlp_task(project)
            # Start index tasks schedulers
            logging.info(f"Scheduling nlp project task: '{project.get('name')}'")
            scheduler.enter(project.get('interval', defInterval), 1, run_nlp_task, (project,))
    
def run_nlp_task(project):
    logging.info(f"Running nlp project '{project.get('name')}' task ...")
    nlpService = nlp.Service(logging, config, idolService)
    # Docano => IDOL
    #nlpService.export_doccano_to_idol(project)
    
    # IDOL => Doccano
    nlpService.export_idol_to_doccano(project)

    # schedule next run
    scheduler.enter(project.get('interval', defInterval), 1, run_nlp_task, (project,))

def schedule_index_tasks():
    for task in config.get('indextasks'):
        if task.get('enabled'):
            if task.get('startrun', False):
                run_index_task(task)
            # Start index tasks schedulers
            logging.info(f"Scheduling index task: '{task.get('name')}'")
            scheduler.enter(task.get('interval', defInterval), 2, run_index_task, (task,))
    
def run_index_task(task):
    logging.info(f"Running index task '{task.get('name')}' ...")
    # INDEX RSS FEEDS
    if task.get('type') == 'rss':
        rssService = rss.Service(logging, task, idolService)
        results = rssService.index_feeds()
        for _r in results:
            logging.info(_r)
    # INDEX STOCK SYMBOLS
    elif task.get('type') == 'stock':
        stockService = stock.Service(logging, task, idolService)
        exchangeCodes = task.get('exchanges', [])
        if len(exchangeCodes) == 0:
            exchangeCodes = stockService.list_exchange_codes()
        stockService.index_stocks_symbols(exchangeCodes)
    # schedule next run
    scheduler.enter(task.get('interval', defInterval), 2, run_index_task, (task,))
    



if __name__ == "__main__":
    plac.call(main)