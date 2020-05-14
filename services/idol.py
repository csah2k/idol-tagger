
import urllib
import requests
import concurrent.futures
from retrying import retry

class Service:


    # http://localhost:9100/a=admin#page/databases
    dah = 'http://localhost:9100'
    dih = 'http://localhost:9101'

    executor = None
    logging = None

    def __init__(self, logging, threads): 
        self.logging = logging
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)

    
    def index_into_idol(self, documents):
        return self.executor.submit(self.index_into_idol_sync, documents).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def index_into_idol_sync(self, documents):
        index_data = ''
        for _d in documents:
            index_data += '\n'.join([
            f"#DREREFERENCE {_d.get('reference')}", 
            f"#DREDBNAME {_d.get('dbname')}"] + 
            [f"#DREFIELD {_f}=\"{_d.get('fields')[_f]}\"" for _f in _d.get('fields')] +
            [f"#DRECONTENT",
            f"{_d.get('content', '')}",
            "#DREENDDOC\n"])
        index_data = index_data + "#DREENDDATAREFERENCE"
        return requests.post(f'{self.dih}/DREADDDATA?', data=index_data.encode('utf-8'), headers={'Content-type': 'text/plain; charset=utf-8'}, verify=False).text.strip()

    def suggest_on_text(self, query={}):
        return self.executor.submit(self.suggest_on_text_sync, query).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def suggest_on_text_sync(self, query):    
        response = requests.get(f"{self.dah}/a=SuggestOnText&ResponseFormat=simplejson&{urllib.parse.urlencode(query)}", verify=False)    
        return response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])

    def query(self, query={}):    
        return self.executor.submit(self.query_sync, query).result()

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def query_sync(self, query):    
        response = requests.get(f"{self.dah}/a=Query&ResponseFormat=simplejson&{urllib.parse.urlencode(query)}", verify=False)    
        return response.json().get('autnresponse', {}).get('responsedata', {}).get('hit', [])
