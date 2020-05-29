
import json
import logging
import services.core as core
from pymongo import MongoClient
from flask import Flask, request, jsonify, Response
from requests.structures import CaseInsensitiveDict
from services.utils import getLogLvl
config = {}
app = Flask(__name__)
coreService:core.Service = None

# TODO - add twitter source   
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

# TODO - do some asserts in configuration, trim all values, and maybe use dataset or TynyDb   
# https://pypi.org/project/tinydb/ 
# https://dataset.readthedocs.io/en/latest/quickstart.html#connecting-to-a-database

# TODO - add metafields manual addition in index tasks configuration

# TODO - service statistics
#self.statistics = sqlite3.connect(dbfile, check_same_thread=False)
#self.statistics.cursor().execute("create table tasks_executions (id, username, type, execution_time, elapsed_seconds, total_scanned, total_indexed)")

with open('config.json') as json_configfile:
    config = CaseInsensitiveDict(json.load(json_configfile))

logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=getLogLvl(config), handlers=[logging.FileHandler(config.get('service',{}).get('logfile','service.log'), 'w', 'utf-8')])
logging.getLogger('elasticsearch').setLevel(logging.ERROR)

coreService = core.Service(logging, config)
coreService.start()

@app.route("/tasks/<username>", methods = ['GET','POST'])
def tasks(username:str):
    ret = {}
    if request.method == 'POST':
        ret = coreService.set_user_task(username, request.json)
    elif request.method == 'GET':
        ret = coreService.get_user_tasks(username)
    return Response(response=ret, status=200, mimetype="application/json")


@app.route("/projects/<username>", methods = ['GET','POST'])
def projects(username:str):
    ret = {}
    if request.method == 'POST':
        ret = {} ## TODO
    elif request.method == 'GET':
        ret = coreService.get_user_projects(username)
    return Response(response=ret, status=200, mimetype="application/json")

@app.route("/indices/<username>", methods = ['GET','POST'])
def indices(username:str):
    ret = {}
    if request.method == 'POST':
        ret = {} ## TODO
    elif request.method == 'GET':
        ret = coreService.get_user_indices(username)
    return Response(response=ret, status=200, mimetype="application/json")







if __name__ == "__main__":
    # DEV FLASK SELF-STARTUP
    with open('config.json') as json_configfile:
        config = CaseInsensitiveDict(json.load(json_configfile))
    # logging setup
    logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=getLogLvl(config), handlers=[logging.FileHandler(config.get('service',{}).get('logfile','service.log'), 'w', 'utf-8')])
    logging.getLogger('elasticsearch').setLevel(logging.ERROR)

    host = config.get('service',{}).get('host','0.0.0.0')
    port = config.get('service',{}).get('port',8080)
    app.run(debug=False, use_reloader=False, host=host, port=port, threaded=True)

    