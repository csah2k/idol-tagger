from __future__ import unicode_literals, print_function

import sys
import json
import logging
import services.idol as idol
import services.rss as rss
import services.stock as stock
import services.doccano as doccano
import services.scheduler as sched
from requests.structures import CaseInsensitiveDict

# TODO - add twitter source   https://python-twitter.readthedocs.io/en/latest/getting_started.html
# TODO - do some asserts in configuration and maybe use TynyDb for the implementation  https://pypi.org/project/tinydb/ 
# TODO - add metafields manual addition in index tasks configuration


logfile='main.log'
config = {}
srvcfg = {}
idolService = None

with open('config.json') as json_configfile:
    config = CaseInsensitiveDict(json.load(json_configfile))
    srvcfg = config.get('service',{})

def main():
    logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=getLogLvl(srvcfg), handlers=[logging.FileHandler(srvcfg.get('logfile', logfile), 'w', 'utf-8')])
    logging.info("============================ Starting  ============================")
    logging.info(srvcfg)
    logging.debug(config)
    
    idolService = idol.Service(logging, config)
    schedService = sched.Service(logging, config, idolService, doccano, rss, stock)
    schedService.start()
    

    
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
    #plac.call(main)
    main()