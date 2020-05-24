from __future__ import unicode_literals, print_function

import sys
import time
import json
import hashlib 
import logging
import services.scheduler as sched
from requests.structures import CaseInsensitiveDict
from pymongo import MongoClient

# TODO - add twitter source   
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

# TODO - do some asserts in configuration, trim all values, and maybe use dataset or TynyDb   
# https://pypi.org/project/tinydb/ 
# https://dataset.readthedocs.io/en/latest/quickstart.html#connecting-to-a-database

# TODO - add metafields manual addition in index tasks configuration

class Service:
    
    def __init__(self, logging, config): 
        self.logging = logging 
        self.config = config
        # database setup
        self.mongo = MongoClient(config['service']['mongodb'])
        self.mongodb = self.mongo['index-flow']
        self.users_profiles = self.mongodb['users_profiles']
        self.profiles_tasks = self.mongodb['profiles_tasks']

    def start(self):
        self.logging.info("============================ Starting index-flow service =================================")
        self.logging.debug(self.config)        


        # tasks scheduler setup
        schedService = sched.Service(self.logging, self.config, self.mongodb)
        schedService.scheduleAllProfilesTasks()

        # MOCK
        #self.setUserProfile("cassio.sa@gmail.com")
        #self.addUserTask("cassio.sa@gmail.com", self.config['TEST_indextasks']['rss'])


        while True:
            self.logging.info("Collecting statistics...")
            _statistics = schedService.statistics()
            # TODO save the statistics somewhere
            time.sleep(30)

    def addUserTask(self, login:str, task:dict):
        query = {"login": login}
        task.update(query)
        self.profiles_tasks.insert_one(task)
        self.logging.info(task)
        self.logging.info(f"Task added: {str(task['_id'])} @ {login}")

    def setUserProfile(self, login:str, profile={}):
        query = {"login": login}
        profile.update(query)
        user_profile = self.config['default_profile'].copy()
        user_profile.update(profile)
        if self.users_profiles.count_documents(query) <= 0:
            # generate indexes names
            user_profile.update({
                "indices": {
                    "indexdata": str(hashlib.md5(("indexdata"+login).encode())).lower(),
                    "filters": str(hashlib.md5(("filters"+login).encode())).lower(),
                }
            })
            self.logging.info(user_profile)
            self.users_profiles.insert_one(user_profile)
            self.logging.info(f"Profile added '{login}'")
        else:
            self.logging.info(user_profile)
            self.users_profiles.update_one(query, {"$set": user_profile})
            self.logging.info(f"Profile updated '{login}'")


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

    