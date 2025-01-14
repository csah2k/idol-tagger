# pylint: disable=unexpected-keyword-arg

import time
import urllib
import logging
import requests
import threading
from datetime import datetime
import concurrent.futures
from retrying import retry
from elasticsearch import Elasticsearch
from requests.structures import CaseInsensitiveDict

import services.utils as util

# https://elasticsearch-py.readthedocs.io/en/master/
# https://github.com/spinscale/elasticsearch-ingest-langdetect
# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax


#self.detect_language("Qbox makes it easy for us to provision an Elasticsearch cluster without wasting time on all the details of cluster configuration.")
''' QUERY
{
    "query_string" : {
        "query" : "(new york city) OR (big apple)",
        "default_field" : "content"
    }
},
'''
class Service:

    def __init__(self, logging, config:dict): 
        self.logging = logging
        self.config = config
        self.cfg = config.get('elastic').copy()
        self.lock = threading.Lock()
        self.index_queues = {}
        self.numthreads = config.get('threads', 2)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.numthreads, thread_name_prefix='ElasticPool')
        self.running = self.executor.submit(self.initService).result()
    
    def initService(self):
        try:
            self.elastic = Elasticsearch([self.cfg.get('api')])
            self.inf = self.elastic.info() 
            self.logging.info(f"ElasticSearch service started [cluster_name: '{self.inf['cluster_name']}', version: {self.inf['version']['number']}, threads: {self.numthreads}]")
            self._create_pipelines()
            return True
        except Exception as error:
            self.logging.error(f"ElasticSearch: {error}")
            return False
    
    def indices_status(self, indices:dict):
        return self.executor.submit(self._indices_status, indices).result()

    def _indices_status(self, indices:dict):  
        return self.elastic.indices.stats(index=','.join([v for k,v in indices.items()]), metric='docs,store')
        
    def initIndices(self, indices:dict):   
        return self.executor.submit(self._initIndices, indices).result()

    def _initIndices(self, indices:dict):  
        for tp, idx in indices.items():
            self._create_index(idx, tp == "filters")
        
    def index_documents(self, documents:list, index:str):
        return self.executor.submit(self._index_documents, documents, index).result()

    def _index_documents(self, documents:list, index:str):
        total = 0
        for doc in documents:
            total += self._index_document(doc, index)
        return total

    def index_document(self, document:dict, index:str, replace=True):
        return self.executor.submit(self._index_document, document, index, replace).result()

    def _index_document(self, document:dict, index:str, replace=True):
        try:
            _id = document.get('_id', document.get('id', document.get('ref', None)))
            if replace:
                self.elastic.index(index=index, body=document, id=_id, pipeline='index-pipeline')
            else:
                res = self.elastic.count(body={
                    "query": {
                        "terms": { "_id": [ _id ]  }
                    }}, index=index)
                if (res or {}).get('count',0) == 0:
                    self.elastic.index(index=index, body=document, id=_id, pipeline='index-pipeline')
                else:
                    return 0
            return 1
        except Exception as error:
            self.logging.error(f"ElasticService: {error}")
            return 0

    def remove_document(self, _id:str, index:str):
        return self.executor.submit(self._remove_document, _id, index).result()

    def _remove_document(self, _id:str, index:str):
        return self.elastic.delete(index, _id)

    def query(self, query:dict, index:str): 
        return self.executor.submit(self._query, query, index).result()

    def _query(self, query:dict, index:str): 
        res = self.elastic.search(index=index, body=query)
        hits = res.get('hits', {}).get('hits', [])
        hits = [dict({'id':hit['_id'], 'index':hit['_index'], 'score':hit['_score']*100.0 }, **hit['_source']) for hit in hits]
        return hits

    def search_filters(self, text:str, index:str):
        return self.executor.submit(self._search_filters, text, index).result()

    def _search_filters(self, text:str, index:str):
        query = {
            "query": {
                "percolate" : {
                    "field" : "query",
                    "document" : {
                        "content" : text
                    }
                }
            }
        }
        return self.query(query, index)

    def addIndexFilter(self, index:str, title:str, query:str):
        return self.executor.submit(self._addIndexFilter, index, title, query).result()

    def _addIndexFilter(self, index:str, title:str, query:str):
        body = {
            "title": title,
            "query": {
                "query_string" : {
                    "query" : query,
                    "fields" : ["content"]
                }
            }
        }
        try:
            res = self.elastic.index(index, body)
            return res
        except Exception as error:
            self.logging.error(f"ElasticSearch: {error}")

    def update_fields(self, index:str, _id:str, fields:dict):
        return self.executor.submit(self._update_fields, index, _id, fields).result()

    def _update_fields(self, index:str, _id:str, fields:dict):
        body = { "doc" : fields  }
        return self.elastic.update(index, _id, body)


    def add_index_field(self, index:str, fieldname:str, _type="keyword"):
        return self.executor.submit(self._add_index_field, index, fieldname, _type).result()

    def _add_index_field(self, index:str, fieldname:str, _type:str):
        curr_map = self.elastic.indices.get_mapping(index=index)
        curr_prop = curr_map.get(index,{}).get('mappings',{}).get('properties',{})
        if curr_prop.get(fieldname, None) != None:
            self.logging.debug(f"field {fieldname} already exists in index {index}")
            return {}
        body = {
            "properties": {
                str(fieldname): {
                    "type": _type
                }
            }
        }
        self.logging.debug(f"creating field {fieldname} in index {index}")
        return self.elastic.indices.put_mapping(body, index=index)

    def create_index(self, index:str, filtr=False):
        return self.executor.submit(self._create_index, index, filtr).result()

    def _create_index(self, index:str, filtr=False):
        if not self.elastic.indices.exists(index):
            body = {
                "settings" : {
                    "number_of_shards" : 1,
                    "number_of_replicas" : 1
                },
                "mappings" : { }
            }
            if filtr:
                body['mappings']['properties'] = {
                    "title" : { "type": "text", "index": False },
                    "query":  { "type": "percolator" },
                    "content":{ "type": "text" }
                }
            else:
                body['mappings']['properties'] = {
                    "id" :   { "type": "keyword" },
                    "title" :   { "type": "text" },
                    "content":  { "type": "text" },  
                    "language": { "type": "keyword"},
                    "date":     { "type": "date" },
                    "url":      { "type": "keyword", "index": False },
                    "src":      { "type": "keyword", "index": False },
                    "indextask":{ "type": "keyword"},
                    "task_id":  { "type": "keyword"},
                    "filter":   { "type": "object"}
                }
            try:
                res = self.elastic.indices.create(index, body=body)
                self.logging.info(f"create index {res}")
                return res
            except Exception as error:
                self.logging.error(f"ElasticSearch: {error}")

    def detect_language(self, text:str):
        return self.executor.submit(self._detect_language, text).result()

    def _detect_language(self, text:str):
        body = {
            "pipeline" :
            {
                "description": "_description",
                "processors": [
                    {
                        "langdetect" : {
                            "field" : "content",
                            "target_field" : "language"
                        }
                    }
                ]
            },
            "docs": [
                {
                    "_index": "index",
                    "_id": "id",
                    "_source": {
                        "content": text
                    }
                }
            ]
        }
        try:
            res = self.elastic.ingest.simulate(body)
            lang = res.get('docs',[{}])[0].get('doc',{}).get('_source',{}).get('language', 'unknown')
            return lang
        except Exception as error:
            self.logging.error(f"ElasticSearch: {error}")
        
    
    def _create_pipelines(self):
        body = {
            "description" : "Indexing pre-processors",
            "processors" : [
                {
                    "date" : {
                        "field" : "date",
                        "target_field" : "date",
                        "formats" : [
                            "dd/MM/yyyy hh:mm:ss", 
                            "E, d MMM yyyy HH:mm:ss Z", 
                            "E, d MMM yyyy HH:mm:ss zzz", 
                            "yyyy-MM-dd'T'HH:mm:ssZ", 
                            "MMM d, h:mm a", 
                            "EEEE, MMM d, yyyy", 
                            "d MMM yyyy H:mm:ss", 
                            "yyyy-MM-dd'T'HH:mm:ssZZZZZ",
                            "dd MMM yyyy HH:mm:ss Z"
                        ],
                        "on_failure" : [
                            {
                                "set" : {
                                    "field" : "date",
                                    "value" : "{{_ingest.timestamp}}"
                                }
                            }
                        ]
                    }
                },
                {
                    "html_strip": {
                        "field": "title"
                    }
                },
                {
                    "html_strip": {
                        "field": "content"
                    }
                },
                {
                    "trim": {
                        "field": "title"
                    }
                },
                {
                    "trim": {
                        "field": "content"
                    }
                },
                {
                    "langdetect" : {
                        "field" : "content",
                        "target_field" : "language",
                        "on_failure" : [
                            {
                                "set" : {
                                    "field" : "language",
                                    "value" : "unknown"
                                }
                            }
                        ]
                    }
                }                
            ]
        }
        
        try:
            res = self.elastic.ingest.put_pipeline('index-pipeline', body)
            self.logging.debug(res)
        except Exception as error:
            self.logging.error(f"ElasticSearch: {error}")