

import time
import sched
import urllib
import requests
import threading
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict
filters_fieldprefix = 'FILTERINDEX'

class Service:

    executor = None
    logging = None
    config = None    
    index_queues = {}
    index_queues_lock = None

    def __init__(self, logging, config): 
        self.logging = logging
        self.config = config.get('idol')
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2))
        self.index_queues_lock = threading.Lock()
        concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix='IdolScheduler').submit(self.init_batch_queue)

    def index_into_idol(self, documents, target_db, priority=0):
        self.executor.submit(self._index_into_idol, documents, target_db, priority)

    @retry(wait_fixed=2000, stop_max_delay=10000)
    def _index_into_idol(self, documents, target_db, priority=0):
        index_data = ''
        for _d in documents:
            fields = _d.get('fields', [])
            content = _d.get('drecontent', '')
            DOCUMENTS = _d.get('content',{}).get('DOCUMENT',[])
            for DOC in DOCUMENTS:
                for key in DOC:
                    if key == 'DRECONTENT':
                        for value in DOC[key]:
                            content += value
                    else:
                        for value in DOC[key]:
                            fields.append((key, value))

            index_data += '\n'.join([
            f"#DREREFERENCE {_d.get('reference')}"] + 
            [f"#DREFIELD {_f[0]}=\"{_f[1]}\"" for _f in fields] +
            [f"#DRECONTENT",
            f"{content}",
            "#DREENDDOC\n\n"])
        #index_data = index_data + "#DREENDDATAREFERENCE"
        query = {
            'DREDbName': target_db,
            'CreateDatabase' : True,
            'Priority': priority
        }
        # add to queue
        self.add_into_batch_queue(query, index_data)


    def init_batch_queue(self):
        self.logging.info(f"Starting idol index batch queue ...")
        batch_sched = sched.scheduler(time.time, time.sleep)
        batch_sched.enter(self.config.get('batchinterval', 5), 1, self.handle_batch_queue, (batch_sched,))
        batch_sched.run()

    def handle_batch_queue(self, scheduler):
        # index batch if full or expired
        self.index_queues_lock.acquire(True)
        current_time = time.time()
        for query_uuid in self.index_queues:
            queue_size = len(self.index_queues[query_uuid])
            queue_time = self.index_queues[query_uuid][queue_size-1][0]
            if queue_size >= self.config.get('batchsize', 100) or (current_time-queue_time) > self.config.get('batchexpire', 30):
                query = self.index_queues[query_uuid][queue_size-1][1]
                self.executor.submit(self.post_index_data, query, self.index_queues[query_uuid].copy())
                self.index_queues[query_uuid].clear()
        self.index_queues_lock.release()
        scheduler.enter(self.config.get('batchinterval', 5), 1, self.handle_batch_queue, (scheduler,))

    def post_index_data(self, query, docs=[]):
        index_data = '\n'.join([_d[2] for _d in docs]) + "\n#DREENDDATAREFERENCE\n\n"
        resp = requests.post(f"{makeUrl(self.config.get('dih'))}/DREADDDATA?{urllib.parse.urlencode(query)}", data=index_data.encode('utf-8'), headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"Indexed batch: {resp}")
        # TODO on error add docs back into the batch queue


    def add_into_batch_queue(self, query, index_data):
        query_uuid = hashDict(query)
        self.index_queues_lock.acquire(True)
        current_time = time.time()
        if query_uuid not in self.index_queues:
            self.index_queues[query_uuid] = [(current_time, query, index_data)]
        else:
            self.index_queues[query_uuid].append((current_time, query, index_data))
        self.logging.debug(f"document added to index batch queue, current queue size: {len(self.index_queues[query_uuid])}")
        self.index_queues_lock.release()

    def remove_documents(self, refers, priority=0):
        return self.executor.submit(self._remove_documents, refers, priority).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _remove_documents(self, refers, priority):
        query = {
            'Docs': ','.join(refers),
            'Priority': priority
        }
        resp = requests.get(f"{makeUrl(self.config.get('dih'))}/DREDELETEREF?{urllib.parse.urlencode(query)}", headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"Removed docs: {len(refers)}, resp: [{resp}]")
        return resp

    def move_to_database(self, source_dbs, target_db, refers=[]):
        return self.executor.submit(self._move_to_database, source_dbs, target_db, refers).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _move_to_database(self, source_dbs, target_db, refers):
        query = {
            'TargetEngineHost': self.config.get('dah').get('host'),
            'TargetEnginePort': self.config.get('dah').get('port'),
            'MatchReference': ','.join(refers),
            'DatabaseMatch': ','.join(source_dbs), 
            'DREDbName': target_db,
            'CreateDatabase': True,
            'Delete' : True
        }
        resp = requests.get(f"{makeUrl(self.config.get('dih'))}/DREEXPORTREMOTE?{urllib.parse.urlencode(query)}", headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"Moved docs: {len(refers)}, resp: [{resp}]")
        return resp

    def suggest_on_text(self, query={}):
        return self.executor.submit(self._suggest_on_text, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _suggest_on_text(self, query):    
        response = requests.get(f"{makeUrl(self.config.get('dah'))}/a=SuggestOnText&ResponseFormat=simplejson&{urllib.parse.urlencode(clearQuery(query))}", verify=False)    
        return response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])

    def query(self, query={}):    
        return self.executor.submit(self._query, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _query(self, query):    
        response = requests.get(f"{makeUrl(self.config.get('dah'))}/a=Query&ResponseFormat=simplejson&{urllib.parse.urlencode(clearQuery(query))}", verify=False)   
        hits = response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])
        self.logging.debug(f"Idol query results: {len(hits)}") 
        return hits

    def detect_language(self, text):    
        return self.executor.submit(self._detect_language, text).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _detect_language(self, text):    
        response = requests.get(f"{makeUrl(self.config.get('dah'))}/a=DetectLanguage&ResponseFormat=simplejson&Text={text}", verify=False)   
        response_data = response.json().get('autnresponse', {}).get('responsedata', {})
        language = response_data.get('language', 'GENERAL') if response_data.get('language') != 'UNKNOWN' else 'GENERAL'
        encoding = response_data.get('languageencoding', 'UTF8')
        return { 'language': language , 'encoding': encoding, 'name': language+encoding  }

    def summarize_text(self, query):    
        return self.executor.submit(self._summarize_text, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _summarize_text(self, query):    
        response = requests.get(f"{makeUrl(self.config.get('dah'))}/a=Summarize&ResponseFormat=simplejson&{urllib.parse.urlencode(clearQuery(query))}", verify=False)   
        response_data = response.json().get('autnresponse', {}).get('responsedata', {})
        return response_data.get('summary', query.get('text', ''))


## --------- helper functions ------------

def makeUrl(component):
    return f"{component.get('protocol','http')}://{component.get('host','localhost')}:{component.get('port',9000)}"


def clearQuery(query):
    query.pop('a', None)
    query.pop('action', None)
    query.pop('responseformat', None)
    return query

def hashDict(dct):
    return str(hash(frozenset(dct.items())))