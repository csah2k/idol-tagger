
import re 
import uuid 
import random
import logging
import datetime
import feedparser
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict

from services.elastic import Service as elasticService
import services.utils as util

class Service:

    def __init__(self, logging, task_cfg, index:elasticService): 
        self.logging = logging 
        self.task_cfg = task_cfg
        self.index = index 
        self.re_http_url = re.compile(r'^.*(https?://.+)$', re.IGNORECASE)   
        self.numthreads = self.task_cfg['params'].get('threads',2)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.numthreads, thread_name_prefix='RssPool')
        self.statistics = { 'threads': self.numthreads, 'feeds': 0, 'errors': 0, 'scanned': 0, 'indexed': 0 }

    def result(self):
        self.executor.shutdown()
        return self.statistics

    def index_feeds(self, max_feeds=0):
        return self.executor.submit(self._index_feeds, max_feeds).result()
        
    def _index_feeds(self, max_feeds=0):
        filename = self.task_cfg['params'].get('feeds', 'data/feeds') # TODO salvar/ler lista de feeds no mongodb
        feeds_file = open(filename, 'r') 
        Lines = feeds_file.readlines() 
        feeds_urls = set()  ## a set assures to no have duplicated url's
        for _l in Lines: 
            _url = _l.strip()
            if self.re_http_url.match(_url):
                feeds_urls.add(_url)
        feeds_file.close()
        feeds_urls = list(feeds_urls)
        random.shuffle(feeds_urls) ## shuffle to avoid flood same domain with all threads at same time
        if max_feeds <= 0: max_feeds = len(feeds_urls)
        feeds_urls = feeds_urls[:max_feeds]
        self.logging.info(f"RSS indextask '{self.task_cfg.get('name')}': Crawling {len(feeds_urls)} urls using {self.numthreads} threads")
        self.statistics.update({'feeds': len(feeds_urls)})
       
        index_threads = []
        for _url in feeds_urls:
            index_threads.append(
                self.executor.submit(self.index_feed, _url, 
                    self.executor.submit(self.get_feed_from_url, _url).result())) 

        total_errors_docs = 0
        total_scanned_docs = 0
        total_indexed_docs = 0
        for _t in index_threads:
            result =  _t.result()
            self.logging.debug(f"{result}")
            total_errors_docs += result.get('errors', 1)
            total_scanned_docs += result.get('scanned', 0)
            total_indexed_docs += result.get('indexed', 0)
            self.statistics.update({'scanned': total_scanned_docs, 'indexed': total_indexed_docs, 'errors': total_errors_docs })
        
        self.logging.info(f"RSS indextask '{self.task_cfg.get('name')}' finished: {self.statistics}")
        return self.statistics

    def index_feed(self, feed_url, feed):
        try:
            if len((feed or {}).get('entries',[])) <= 0:
                raise Exception('no feed entries')
            return self._index_feed(feed_url, feed)
        except Exception as error:
            self.logging.error(f"RSS_URL: {feed_url} | {str(error)}")
            return { 'url': feed_url, 'error': str(error) }

    def _index_feed(self, feed_url, feed):      
        total_errors_docs = 0
        total_indexed_docs = 0
        for _e in feed.get('entries',[]):
            link = None
            try:
                indices = self.task_cfg['user']['indices']
                link = _e.get('link', _e.get('href', _e.get('url', _e.get('links', [{'href':feed_url}])[0].get('href', feed_url) )))
                if self.re_http_url.match(link): link = self.re_http_url.search(link).group(1)
                _id = uuid.uuid3(uuid.NAMESPACE_URL, link)
                date = _e.get('published', _e.get('timestamp', _e.get('date')))
                content = _e.get('summary', _e.get('description', _e.get('text', _e.get('content', _e.get('paragraph', None)))))
                title = util.cleanText(_e.get('title', _e.get('titulo', _e.get('headline', content or ''))))
                if content == None and title != None: content = title
                else: content = util.cleanText(content)
                if len(title) > 100: title = title[:100].rsplit(' ', 1)[0]+'...' # truncate title
            
                filterHits = []
                if self.task_cfg.get('filters', False): # TODO ser possivel escolher quais filtros aplicar
                    filterHits = self.index.search_filters(content, indices['filters'])

                if not self.task_cfg.get('filters', False) or len(filterHits) > 0:
                    doc = {
                        '_id': _id,
                        'title': title,
                        'content': content,
                        'date': date,
                        'url': link,
                        'src': feed_url,
                        'indextask': self.task_cfg.get('name'),
                        'task_id': str(self.task_cfg.get('_id', self.task_cfg.get('id'))),
                        'filter': {}
                    }
                    for hit in filterHits:
                        doc['filter'][hit.get('id')] = hit.get('title')
                    total_indexed_docs += self.index.index_document(doc, indices['indexdata'])
            except Exception as error:
                self.logging.error(f"feed entry: {link} -> error: {str(error)}")
                total_errors_docs += 1

        return { 'url': feed_url, 'scanned': len(feed.entries), 'indexed': total_indexed_docs, 'errors': total_errors_docs }

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def get_feed_from_url(self, feed_url):
        return feedparser.parse(feed_url)



