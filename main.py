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
idolService = None
#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
with open('config.json') as json_configfile:
    config = json.load(json_configfile)
    srvcfg = config.get('service',{})

def main():
    global idolService
    logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=getLogLvl(srvcfg), handlers=[logging.FileHandler(srvcfg.get('logfile', logfile), 'w', 'utf-8')])
    logging.info("==============> Starting service... ")
    logging.info(srvcfg)
    idolService = idol.Service(logging, config)

    # TODO separar toda a implementação de scheduling e execução para um novo serviço 'scheduler'
    nlpTasksSched = sched.scheduler(time.time, time.sleep)
    indexTasksSched = sched.scheduler(time.time, time.sleep)
    
    schedule_nlp_tasks(nlpTasksSched)
    schedule_index_tasks(indexTasksSched)
    
    nlpTasksSched.run()
    indexTasksSched.run()


def schedule_nlp_tasks(scheduler):
    for task in config.get('nlp',{}).get('projects', []):
        if task.get('enabled'):
            if task.get('startrun', False):
                run_nlp_task(task, None)
            # Start index tasks schedulers
            logging.info(f"Scheduling nlp project task: '{task.get('name')}'")
            scheduler.enter(task.get('interval', defInterval), 1, run_nlp_task, (task, scheduler))
    
def run_nlp_task(task, scheduler):
    logging.info(f"Running nlp project '{task.get('name')}' task ...")
    nlpService = nlp.Service(logging, config, idolService)

    # IDOL => Doccano
    nlpService.export_idol_to_doccano(task)

    # Docano => IDOL
    nlpService.export_doccano_to_idol(task)

    # schedule next run
    if scheduler != None:
        scheduler.enter(task.get('interval', defInterval), 1, run_nlp_task, (task, scheduler))

def schedule_index_tasks(scheduler):
    for task in config.get('indextasks'):
        if task.get('enabled'):
            if task.get('startrun', False):
                run_index_task(task, None)
            # Start index tasks schedulers
            logging.info(f"Scheduling index task: '{task.get('name')}'")
            scheduler.enter(task.get('interval', defInterval), 2, run_index_task, (task, scheduler))
    
def run_index_task(task, scheduler):
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
    if scheduler != None:
        scheduler.enter(task.get('interval', defInterval), 2, run_index_task, (task, scheduler))
    
def getLogLvl(cfg):
    lvl = cfg.get('loglevel', 'INFO').upper()
    loglvl = logging.INFO if lvl == 'INFO' else None
    if loglvl == None: loglvl = logging.DEBUG if lvl == 'DEBUG' else None
    if loglvl == None: loglvl = logging.WARN if lvl == 'WARN' else None
    if loglvl == None: loglvl = logging.WARNING if lvl == 'WARNING' else None
    if loglvl == None: loglvl = logging.ERROR if lvl == 'ERROR' else None
    if loglvl == None: loglvl = logging.FATAL if lvl == 'FATAL' else None
    return loglvl



if __name__ == "__main__":
    plac.call(main)