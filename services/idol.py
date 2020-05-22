

import time
import urllib
import logging
import requests
import threading
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict

import services.utils as util

class Service:

    def __init__(self, logging, config): 
        self.logging = logging
        self.config = config.get('idol').copy()
        self.lock = threading.Lock()
        self.index_queues = {}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2), thread_name_prefix='IdolPool')
        self.executor.submit(self.init_batch_queue)

    def index_into_idol(self, documents, query):
        self.executor.submit(self._index_into_idol, documents, query)

    @retry(wait_fixed=1000, stop_max_delay=10000)
    def _index_into_idol(self, documents, query):
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
        # add to queue
        _query = CaseInsensitiveDict(query)
        if _query.get('priority', 0) >= 100: # bypass queue
            self.post_index_data(_query, [(None, _query, index_data, len(documents))])
        else:
            self.add_into_batch_queue(_query, index_data, len(documents))

    def set_field_value(self, references, field, value, query={}):
        self.executor.submit(self._set_field_value, references, field, value, query)

    def _set_field_value(self, references, field, value, query={}):
        index_data = ''
        for reference in references:
            index_data += '\n'.join([
                f"#DREDOCREF {reference}",
                f"#DREFIELDNAME {field}",
                f"#DREFIELDVALUE {value}"])
        index_data += "\n#DREENDDATAREFERENCE\n\n"
        resp = requests.post(f"{util.makeUrl(self.config.get('dih'))}/DREREPLACE?{urllib.parse.urlencode(query)}", data=index_data.encode('utf-8'), headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"set_field_value: {resp}")

    @retry(wait_fixed=2000, stop_max_delay=60000)
    def post_index_data(self, query, docs=[]):
        try:
            batchsize = sum([_d[3] for _d in docs])
            index_data = '\n'.join([_d[2] for _d in docs]) + "\n#DREENDDATAREFERENCE\n\n"
            resp = requests.post(f"{util.makeUrl(self.config.get('dih'))}/DREADDDATA?{urllib.parse.urlencode(query)}", data=index_data.encode('utf-8'), headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
            self.logging.info(f"Batch sent [docs:{batchsize}, resp:{resp}, pri:{query.get('priority',0)}]")
        except Exception as error:
            self.logging.error(f"post_index_data error: {str(error)}, docs:{len(docs)},  query: {query}")

    def remove_documents(self, references, dbname, priority=0):
        return self.executor.submit(self._remove_documents, references, dbname, priority)

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _remove_documents(self, references, dbname, priority=0):
        docs = '+'.join(references)
        resp = requests.post(f"{util.makeUrl(self.config.get('dih'))}/DREDELETEREF?Priority={priority}&DREDbName={dbname}&Docs={docs}", headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"Removed refs {len(references)} in {dbname}, resp: {resp}")
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
        resp = requests.get(f"{util.makeUrl(self.config.get('dih'))}/DREEXPORTREMOTE?{urllib.parse.urlencode(query)}", headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"Moved docs: {len(refers)}, resp: [{resp}]")
        return resp

    def suggest_on_text(self, query={}):
        return self.executor.submit(self._suggest_on_text, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _suggest_on_text(self, query): 
        response = requests.post(f"{util.makeUrl(self.config.get('dah'))}/a=SuggestOnText&ResponseFormat=simplejson", data=util.aciQuery(query), verify=False)    
        return response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])

    def query(self, query={}):    
        return self.executor.submit(self._query, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _query(self, query): 
        response = requests.post(f"{util.makeUrl(self.config.get('dah'))}/a=Query&ResponseFormat=simplejson", data=util.aciQuery(query), verify=False)   
        hits = response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])
        return hits

    def get_statetoken(self, query={}):    
        return self.executor.submit(self._get_statetoken, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _get_statetoken(self, query):    
        params = {
            'Print': 'NoResults',
            'StoreState': True,
            'StoredStateField': 'DREREFERENCE',
            'StoredStateTokenLifetime': 600
        }
        response = requests.post(f"{util.makeUrl(self.config.get('dah'))}/a=Query&ResponseFormat=simplejson", data=util.aciQuery(query, params), verify=False)   
        statetokeid = response.json().get('autnresponse', {}).get('responsedata', {}).get('state', '')
        self.logging.debug(f"Idol statetoke id: {statetokeid}") 
        return statetokeid

    def get_content(self, query={}):    
        return self.executor.submit(self._get_content, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _get_content(self, query):
        response = requests.post(f"{util.makeUrl(self.config.get('dah'))}/a=GetContent&ResponseFormat=simplejson", data=util.aciQuery(query), verify=False)   
        hits = response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])
        return hits[0] if len(hits) > 0 else None

    def detect_language(self, text):    
        return self.executor.submit(self._detect_language, text).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _detect_language(self, text):    
        response = requests.post(f"{util.makeUrl(self.config.get('dah'))}/a=DetectLanguage&ResponseFormat=simplejson", data={'Text':text}, verify=False)   
        response_data = response.json().get('autnresponse', {}).get('responsedata', {})
        language = response_data.get('language', util.DFLT_LANGUAGE) if response_data.get('language') != 'UNKNOWN' else util.DFLT_LANGUAGE
        encoding = response_data.get('languageencoding', util.DFLT_ENCODE)
        return { 'language': language , 'encoding': encoding, 'name': language+encoding }

    def summarize_text(self, query):    
        return self.executor.submit(self._summarize_text, query).result()

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def _summarize_text(self, query):    
        response = requests.post(f"{util.makeUrl(self.config.get('dah'))}/a=Summarize&ResponseFormat=simplejson", data=util.aciQuery(query), verify=False)  
        response_data = response.json().get('autnresponse', {}).get('responsedata', {})
        return response_data.get('summary', query.get('text', ''))

    @retry(wait_fixed=10000)
    def init_batch_queue(self):
        self.handle_batch_queue()    
        raise Exception('waiting_for_lock')

    def handle_batch_queue(self):
        self.lock.acquire()
        try:
            current_time = time.time()
            for query_uuid in self.index_queues:
                batchsize = sum([_d[3] for _d in self.index_queues[query_uuid]])
                queue_size = len(self.index_queues[query_uuid])
                queue_time = self.index_queues[query_uuid][queue_size-1][0]
                if batchsize >= self.config.get('batchsize', 100) or (current_time-queue_time) > 30:
                    query = self.index_queues[query_uuid][queue_size-1][1]
                    self.executor.submit(self.post_index_data, query.copy(), self.index_queues[query_uuid].copy())
                    self.index_queues[query_uuid].clear()
        finally:
            self.lock.release()
            
    def add_into_batch_queue(self, query, index_data, batchsize):
        self.lock.acquire()
        try:
            query_uuid = util.hashDict(query)
            current_time = time.time()
            if query_uuid not in self.index_queues:
                self.index_queues[query_uuid] = [(current_time, query, index_data, batchsize)]
            else:
                self.index_queues[query_uuid].append((current_time, query, index_data, batchsize))
            self.logging.debug(f"add_into_batch_queue: {batchsize}, queue batches: {len(self.index_queues[query_uuid])}")
        finally:
            self.lock.release()


