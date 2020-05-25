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
from flask import Flask, request, jsonify
from pymongo import MongoClient


# TODO - add twitter source   
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

# TODO - service statistics
#self.statistics = sqlite3.connect(dbfile, check_same_thread=False)
#self.statistics.cursor().execute("create table tasks_executions (id, username, type, execution_time, elapsed_seconds, total_scanned, total_indexed)")

class Service:
    
    def __init__(self, logging, config): 
        logging.info("Starting services ...")
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
        self.scheduler.start()

        #while True:
        #    statistics = self.scheduler.statistics()
        #    self.logging.info(f"scheduler statistics: {statistics}")
        #    # TODO save the statistics somewhere
        #    time.sleep(60)

    def setup_admin_usertasks(self, username='admin'):
        query = {"username":username}
        tasks = self.config.get('admin_tasks',[])
        for task in tasks:
            task.update(query)
            util.set_user_task(self, username, task)   

    def get_user_tasks(self, username:str):
        return util.dumps({ "tasks": list(self.mongo_tasks.find({"username":username})) })
    
    def set_user_task(self, username:str, task:dict):
        return util.set_user_task(self, username, task)

