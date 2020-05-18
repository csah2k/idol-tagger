from __future__ import unicode_literals, print_function

import sys
import plac
import time
import json
import logging
import services.idol as idol
import services.stock as stock
import services.rss as rss
import services.nlp as nlp
import services.scheduler as sched
from requests.structures import CaseInsensitiveDict

# TODO - add twitter source
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

sys.path.insert(0, './services')
logfile='main.log'
config = {}
srvcfg = {}
idolService = None
#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
with open('config.json') as json_configfile:
    config = json.load(json_configfile)
    srvcfg = config.get('service',{})

def main():
    logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=getLogLvl(srvcfg), handlers=[logging.FileHandler(srvcfg.get('logfile', logfile), 'w', 'utf-8')])
    logging.info("==============> Starting service")
    logging.info(srvcfg)

    idolService = idol.Service(logging, config)
    schedService = sched.Service(logging, config, idolService, nlp, rss, stock)

    while True:
        time.sleep(1)
        logging.debug(schedService.statistics())

    
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