from __future__ import unicode_literals, print_function

import sys
import time
import json

import logging
import services.utils as util
import services.elastic as elastic
import services.doccano as doccano
import services.scheduler as scheduler
from requests.structures import CaseInsensitiveDict
from pymongo import MongoClient


# TODO - add twitter source   
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

# TODO - do some asserts in configuration, trim all values, and maybe use dataset or TynyDb   
# https://pypi.org/project/tinydb/ 
# https://dataset.readthedocs.io/en/latest/quickstart.html#connecting-to-a-database

# TODO - add metafields manual addition in index tasks configuration

# TODO - service statistics
#self.statistics = sqlite3.connect(dbfile, check_same_thread=False)
#self.statistics.cursor().execute("create table tasks_executions (id, username, type, execution_time, elapsed_seconds, total_scanned, total_indexed)")


class Service:
    
    def __init__(self, logging, config): 
        logging.info("============================ Starting index-flow service =================================")
        self.logging = logging 
        self.config = config
        # database setup
        self.mongodb = MongoClient(config['service']['mongodb'])[config['service']['database']]
        self.mongo_tasks = self.mongodb['tasks']
        self.mongo_users = self.mongodb['users']
        self.mongo_projects = self.mongodb['projects']
        # core services setup
        self.index = elastic.Service(self.logging, self.config)
        self.doccano = doccano.Service(self.logging, self.config, self.mongodb, self.index)
        self.scheduler = scheduler.Service(self.logging, self.config, self.mongodb, self.doccano, self.index)

    def start(self):
        # tasks scheduler setup
        self.setup_admin_usertasks()

        # MOCK
        #self.add_user_task("csah2k", self.config['TEST_indextasks']['rss'])

        while True:
            #statistics = self.scheduler.statistics()
            #self.logging.info(f"scheduler statistics: {statistics}")
            # TODO save the statistics somewhere
            time.sleep(60)

    def add_user_task(self, username:str, task:dict):
        query = {"username": username}
        task.update(query)
        task.update({
            "lastruntime": 0,
            "nextruntime": 0 if task.get('startrun',False) else int(time.time())+task.get('interval',60),
            "running": False,
            "error": None
        })
        self.mongo_tasks.insert_one(task)
        self.logging.info(f"Task added: '{util.getTaskName(task)}' @ '{username}'")

    def setup_admin_usertasks(self, username='admin'):
        query = {"username":username}
        self.mongo_tasks.delete_many(query)
        tasks = self.config.get('admin_tasks',[])
        for task in tasks:
            task.update(query)
            self.add_user_task(username, task)   


def main():
    config = {}
    with open('config.json') as json_configfile:
        config = CaseInsensitiveDict(json.load(json_configfile))

    # logging setup
    logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=getLogLvl(config), handlers=[logging.FileHandler(config.get('service',{}).get('logfile','service.log'), 'w', 'utf-8')])
    logging.getLogger('elasticsearch').setLevel(logging.ERROR)

    # main service startup
    indexFlowService = Service(logging, config)
    indexFlowService.start()

    
def getLogLvl(cfg):
    lvl = cfg.get('service',{}).get('loglevel', 'INFO').strip().upper()
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

    