
import re 
import time
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

    def __init__(self, logging, config, index:elasticService): 
        self.logging = logging 
        self.config = config.copy()
        self.re_http_url = re.compile(r'^.*(https?://.+)$', re.IGNORECASE)   
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2), thread_name_prefix='RssPool')
        self.index = index 
        self.statistics = { 'threads': self.config.get('threads', 2), 'feeds': 0, 'scanned': 0, 'indexed': 0, 'elapsed_seconds': 0 }

    def getStatitics(self):
        return self.statistics.copy()

    def index_feeds(self, indices, max_feeds=0):
        self.executor.submit(self._index_feeds, indices, max_feeds).result()
        self.executor.shutdown()
        
    def _index_feeds(self, indices, max_feeds=0):
        self.logging.info(f"==== Starting ====>  RSS indextask '{self.config.get('name')}'")
        start_time = time.time()
        filename = self.config.get('feeds', 'data/feeds')
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
        self.logging.info(f"Crawling {len(feeds_urls)} urls using {self.config.get('threads', 2)} threads")
        self.statistics.update({'feeds': len(feeds_urls)})
       
        index_threads = []
        for _url in feeds_urls:
            index_threads.append(
                self.executor.submit(self.index_feed, indices, _url, 
                    self.executor.submit(self.get_feed_from_url, _url).result())) 

        total_process_docs = 0
        total_indexed_docs = 0
        for _t in index_threads:
            result =  _t.result()
            self.logging.debug(f"{result}")
            total_process_docs += result.get('total', 0)
            total_indexed_docs += result.get('indexed', 0)
            elapsed_time = int(time.time() - start_time)
            self.statistics.update({'scanned': total_process_docs, 'indexed': total_indexed_docs, 'elapsed_seconds': elapsed_time })
        
        self.logging.info(f"RSS indextask '{self.config.get('name')}' finished: {self.statistics}")
        return self.statistics

    def index_feed(self, indices, feed_url, feed):
        try:
            return self._index_feed(indices, feed_url, feed)
        except Exception as error:
            self.logging.error(f"RSS_URL: {feed_url} | {str(error)}")
            return { 'url': feed_url, 'error': str(error) }

    def _index_feed(self, indices, feed_url, feed):      
        total_indexed_docs = 0
        for _e in feed.entries:
            link = None
            try:
                link = _e.get('link', _e.get('href', _e.get('url', _e.get('links', [{'href':feed_url}])[0].get('href', feed_url) )))
                if self.re_http_url.match(link): link = self.re_http_url.search(link).group(1)
                reference = uuid.uuid3(uuid.NAMESPACE_URL, link)
                date = _e.get('published', _e.get('timestamp', _e.get('date')))
                content = util.cleanText(_e.get('summary', _e.get('description', _e.get('text',''))))
                title = util.cleanText(_e.get('title', _e.get('titulo', _e.get('headline', content)))) # TODO truncate big titles

                if self.config.get('filters', False):
                    filterHits = self.index.search_filters(content, indices.get('filters'))

                if not self.config.get('filters', False) or len(filterHits) > 0:
                    doc = {
                        'reference': reference,
                        'title': title,
                        'content': content,
                        'date': date,
                        'url': link,
                        'src': feed_url,
                        'indextask': self.config.get('name'),
                        'filter': {}
                    }
                    for hit in filterHits:
                        doc['filter'][hit.get('id')] = hit.get('title')
                    total_indexed_docs += self.index.index_document(doc, indices.get('indexdata'))
            except Exception as error:
                self.logging.error(f"ENTRY_URL: {link} | {str(error)}")

        return { 'url': feed_url, 'total': len(feed.entries), 'indexed': total_indexed_docs }

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def get_feed_from_url(self, feed_url):
        return feedparser.parse(feed_url)



