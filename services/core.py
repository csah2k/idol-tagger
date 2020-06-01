from __future__ import unicode_literals, print_function

import sys
import time
import json
import logging
import pymongo
import concurrent.futures
import services.utils as util
import services.elastic as elastic
import services.doccano as doccano
import services.scheduler as scheduler
from requests.structures import CaseInsensitiveDict
from flask import Flask, request, jsonify



# TODO - add twitter source   
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

# TODO - service statistics
#self.statistics = sqlite3.connect(dbfile, check_same_thread=False)
#self.statistics.cursor().execute("create table tasks_executions (id, username, type, execution_time, elapsed_seconds, total_scanned, total_indexed)")

class Service:
    
    def __init__(self, logging, config): 
        #numthreads = config['service'].get('threads',4) 
        self.running = False
        self.logging = logging 
        self.config = config
        self.tasks_defaults = config.get('tasks_defaults',{})
        # database setup
        self.mongodb = pymongo.MongoClient(config['service']['mongodb'])[config['service']['database']]
        # db tables
        self.mongo_tasks = self.mongodb['tasks']
        self.mongo_users = self.mongodb['users']
        self.mongo_roles = self.mongodb['roles']
        self.mongo_labels = self.mongodb['labels']
        self.mongo_projects = self.mongodb['projects']
        self.mongo_documents = self.mongodb['documents']
        self.mongo_role_mappings = self.mongodb['role_mappings']
        # db indices
        self.mongo_tasks.create_index([("enabled", 1), ("username", 1), ("projectid", 1), ("nextruntime", -1)])
        self.mongo_documents.create_index([("projectid", 1), ("id", 1)])
        self.mongo_users.create_index([("username", 1), ("id", 1)])
        self.mongo_role_mappings.create_index([("id", 1)])
        self.mongo_roles.create_index([("name", 1)])
        self.mongo_labels.create_index([("id", 1)])
        self.mongo_projects.create_index([("id", 1)])
        # core services setup
        self.index = elastic.Service(self.logging, self.config)
        self.doccano = doccano.Service(self.logging, self.config, self.mongodb, self.index)
        self.scheduler = scheduler.Service(self.logging, self.config, self.mongodb, self.doccano, self.index)
        if self.index.running and self.doccano.running:
            logging.info(f"=========== All services running! ===========")
            self.running = True
        else:
            er = f"Required services not running! [Elastic running: {self.index.running}, Doccano running: {self.doccano.running}]"
            logging.error(er)

    def start(self):
        if self.running:
            # tasks scheduler setup
            self.setup_system_tasks()
            self.scheduler.start()

    def setup_system_tasks(self):
        tasks = self.config.get('system_tasks',[])
        for task in tasks:
            util.set_user_task(self, self.doccano.login['username'], task) 

    def get_user_indices(self, username:str):
        indices = self.mongo_users.find_one({'username': username}).get('indices',{})
        ret = self.index.indices_status(indices)
        return util.JSONEncoder().encode(ret)

    def get_user_tasks(self, username:str):
        return util.JSONEncoder().encode(list(self.mongo_tasks.find({"username":username})))
      
    def set_user_task(self, username:str, task:dict):
        return util.JSONEncoder().encode(util.set_user_task(self, username, task))

    def get_user_projects(self, username:str, sort='nextruntime', order=-1):
        user_id = self.mongo_users.find_one({'username': username}).get('id',None)
        if user_id != None:
            return util.JSONEncoder().encode([_p for _p in self.mongo_projects.aggregate([
                {
                    "$lookup":
                    {
                        "from": "tasks",
                        "localField": "id",
                        "foreignField": "projectid",
                        "as": "project_tasks"
                    }
                },
                {'$match':{'users': {"$in":[user_id]}}},
                {'$sort': { sort: order } }
            ]) ])
        return util.JSONEncoder().encode([])
        
