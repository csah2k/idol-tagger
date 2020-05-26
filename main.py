
import json
import logging
import services.core as core
from pymongo import MongoClient
from flask import Flask, request, jsonify, Response
from requests.structures import CaseInsensitiveDict


# TODO - add twitter source   
# https://python-twitter.readthedocs.io/en/latest/getting_started.html

# TODO - do some asserts in configuration, trim all values, and maybe use dataset or TynyDb   
# https://pypi.org/project/tinydb/ 
# https://dataset.readthedocs.io/en/latest/quickstart.html#connecting-to-a-database

# TODO - add metafields manual addition in index tasks configuration

# TODO - service statistics
#self.statistics = sqlite3.connect(dbfile, check_same_thread=False)
#self.statistics.cursor().execute("create table tasks_executions (id, username, type, execution_time, elapsed_seconds, total_scanned, total_indexed)")


config = {}
app = Flask(__name__)
coreService:core.Service = None

@app.before_first_request
def start_core_service():
    # main service startup
    global coreService
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
    with open('config.json') as json_configfile:
        config = CaseInsensitiveDict(json.load(json_configfile))
    # logging setup
    logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=getLogLvl(config), handlers=[logging.FileHandler(config.get('service',{}).get('logfile','service.log'), 'w', 'utf-8')])
    logging.getLogger('elasticsearch').setLevel(logging.ERROR)

    host = config.get('service',{}).get('host','0.0.0.0')
    port = config.get('service',{}).get('port',8080)
    app.run(debug=False, use_reloader=False, host=host, port=port, threaded=False)

    