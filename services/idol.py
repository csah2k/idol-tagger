
import urllib
import requests
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict

class Service:

    executor = None
    logging = None
    config = None    

    def __init__(self, logging, config): 
        self.logging = logging
        self.config = config.get('idol')
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2))

    def index_into_idol(self, documents, target_db, priority=0):
        return self.executor.submit(self.index_into_idol_sync, documents, target_db, priority).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def index_into_idol_sync(self, documents, target_db, priority):
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
            "#DREENDDOC\n"])
        index_data = index_data + "#DREENDDATAREFERENCE"
        query = {
            'DREDbName': target_db,
            'CreateDatabase' : True,
            'Priority': priority
        }
        resp = requests.post(f"{makeUrl(self.config.get('dih'))}/DREADDDATA?{urllib.parse.urlencode(query)}", data=index_data.encode('utf-8'), headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"Indexed docs: {len(documents)}, resp: [{resp}]")
        return resp

    def remove_documents(self, refers, priority=0):
        return self.executor.submit(self.remove_documents_sync, refers, priority).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def remove_documents_sync(self, refers, priority):
        query = {
            'Docs': ','.join(refers),
            'Priority': priority
        }
        resp = requests.get(f"{makeUrl(self.config.get('dih'))}/DREDELETEREF?{urllib.parse.urlencode(query)}", headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()
        self.logging.info(f"Removed docs: {len(refers)}, resp: [{resp}]")
        return resp

    def move_to_database(self, source_dbs, target_db, refers=[]):
        return self.executor.submit(self.move_to_database_sync, source_dbs, target_db, refers).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def move_to_database_sync(self, source_dbs, target_db, refers):
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
        return self.executor.submit(self.suggest_on_text_sync, query).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def suggest_on_text_sync(self, query):    
        response = requests.get(f"{makeUrl(self.config.get('dah'))}/a=SuggestOnText&ResponseFormat=simplejson&{urllib.parse.urlencode(clearQuery(query))}", verify=False)    
        return response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])

    def query(self, query={}):    
        return self.executor.submit(self.query_sync, query).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def query_sync(self, query):    
        response = requests.get(f"{makeUrl(self.config.get('dah'))}/a=Query&ResponseFormat=simplejson&{urllib.parse.urlencode(clearQuery(query))}", verify=False)   
        hits = response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])
        self.logging.debug(f"Idol query results: {len(hits)}") 
        return hits

    def detect_language(self, text):    
        return self.executor.submit(self.detect_language_sync, text).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def detect_language_sync(self, text):    
        response = requests.get(f"{makeUrl(self.config.get('dah'))}/a=DetectLanguage&ResponseFormat=simplejson&Text={text}", verify=False)   
        response_data = response.json().get('autnresponse', {}).get('responsedata', {})
        language = response_data.get('language', 'GENERAL') if response_data.get('language') != 'UNKNOWN' else 'GENERAL'
        encoding = response_data.get('languageencoding', 'UTF8')
        return { 'language': language , 'encoding': encoding }



## --------- helper functions ------------

def makeUrl(component):
    return f"{component.get('protocol','http')}://{component.get('host','localhost')}:{component.get('port',9000)}"


def clearQuery(query):
    query.pop('a', None)
    query.pop('action', None)
    query.pop('responseformat', None)
    return query