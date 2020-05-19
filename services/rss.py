
import re 
import time
import uuid 
import html
import random
import logging
import datetime
import html2text
import feedparser
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict
filters_fieldprefix = 'FILTERINDEX'

class Service:

    re_http_url = re.compile(r'^.*(https?://.+)$', re.IGNORECASE)    
    #text_maker = None
    executor = None
    logging = None
    config = None
    idol = None

    def __init__(self, logging, config, idol): 
        self.logging = logging 
        self.config = config
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2), thread_name_prefix='RssPool')
        self.idol = idol 
        #self.text_maker = html2text.HTML2Text()
        #self.text_maker.ignore_links = True
        #self.text_maker.ignore_images = True

    def index_feeds(self, filename=None):
        self.executor.submit(self._index_feeds, filename)
        
    def _index_feeds(self, filename=None):
        self.logging.info(f"Starting RSS indextask '{self.config.get('name')}'")
        if filename == None: filename = self.config.get('feeds', 'data/feeds')
        feeds_file = open(filename, 'r') 
        Lines = feeds_file.readlines() 
        feeds_urls = set()  ## a set assures to no have duplicated url's
        for _l in Lines: 
            _url = _l.strip()
            if self.re_http_url.match(_url):
                feeds_urls.add(_url)
        feeds_file.close()
        feeds_urls = list(feeds_urls)
        self.logging.info(f"Crawling {len(feeds_urls)} rss feeds urls")

        random.shuffle(feeds_urls) ## shuffle to avoid flood same domain with all threads at same time
        feeds_threads = []
        for _url in feeds_urls:
            feeds_threads.append((_url, self.executor.submit(self.get_feed_from_url, _url)))

        self.logging.info(f"Processing {len(feeds_threads)} feeds using {self.config.get('threads', 2)} threads")
        index_threads = []
        while len(feeds_threads) > 0:
            finished = []
            running = []
            for _t in feeds_threads:
                if _t[1].done(): finished.append(_t)
                else: running.append(_t)
            for _t in finished:
                index_threads.append(self.executor.submit(self.index_feed, _t[0], _t[1].result()))
            feeds_threads = running
            time.sleep(0.500)

        total_process_docs = 0
        total_indexed_docs = 0
        for _t in index_threads:
            total_process_docs += _t.result().get('total', 0)
            total_indexed_docs += _t.result().get('indexed', 0)

        self.logging.info(f"RSS indextask '{self.config.get('name')}' completed")
        return { 'feeds': len(feeds_urls), 'total': total_process_docs, 'indexed': total_indexed_docs, 'threads': self.config.get('threads', 2) , 'time': 'TODO' }

 
    @retry(wait_fixed=10000, stop_max_delay=30000)
    def get_feed_from_url(self, feed_url):
        return feedparser.parse(feed_url)

    def index_feed(self, feed_url, feed):
        try:
            return self._index_feed(feed_url, feed)
        except Exception as error:
            self.logging.error(f"RSS_URL: {feed_url} | {str(error)}")
            return { 'url': feed_url, 'error': str(error) }

    def _index_feed(self, feed_url, feed):      
        docsToIndex = []
        for _e in feed.entries:
            link = None
            try:
                link = _e.get('link', _e.get('href', _e.get('url', _e.get('links', [{'href':feed_url}])[0].get('href', feed_url) )))
                if self.re_http_url.match(link): link = self.re_http_url.search(link).group(1)
                self.logging.debug(f"processing feed entry: {link}")
                reference = uuid.uuid3(uuid.NAMESPACE_URL, link)
                date = _e.get('published', _e.get('timestamp', _e.get('date')))
                summr = self.cleanText(_e.get('summary', _e.get('description', _e.get('text',''))))
                title = self.cleanText(_e.get('title', _e.get('titulo', _e.get('headline', summr))))
                
                content = f"{title}\n{summr}"
                lang_info= self.idol.detect_language(content)
                
                summQuery = {
                    'Summary': 'Concept',
                    'Sentences': 2,
                    'LanguageType': lang_info.get('name'),
                    'Text': title
                }   
                title = self.idol.summarize_text(summQuery)

                idolHits = []
                for _query in self.config.get('filters'):
                    idolQuery = _query.copy()
                    idolQuery.update({
                        'Text': content,
                        'SingleMatch': True,
                        'IgnoreSpecials': True,
                    })
                    idolHits += self.idol.query(idolQuery)
                
                if len(idolHits) > 0:
                    docsToIndex.append({
                        'reference': reference,
                        'drecontent': content,
                        'fields': [
                            ('LANGUAGE', lang_info.get('name')),
                            ('DATE', date),
                            ('TITLE', title),
                            ('SUMMARY', summr),
                            ('URL', link),
                            ('FEED', feed_url) ]
                        + [(f'{filters_fieldprefix}_DBS', _hit.get('database')) for _hit in idolHits] 
                        + [(f'{filters_fieldprefix}_LNKS', _hit.get('links')) for _hit in idolHits]
                        + [(f'{filters_fieldprefix}_REFS', _hit.get('reference')) for _hit in idolHits]
                    })
            except Exception as error:
                self.logging.error(f"ENTRY_URL: {link} | {str(error)}")

        if len(docsToIndex) > 0:
            query = {
                'DREDbName': self.config.get('database'),
                'KillDuplicates': 'REFERENCE=2', ## check for the same reference in ALL databases
                'CreateDatabase': True,
                'KeepExisting': True, ## do not replace content for matched references in KillDuplicates
                'Priority': 0
            }
            self.idol.index_into_idol(docsToIndex, query)

        return { 'url': feed_url, 'total': len(feed.entries), 'indexed': len(docsToIndex) }

    def cleanText(self, text):
        text_maker = html2text.HTML2Text()
        text_maker.ignore_links = True
        text_maker.ignore_images = True
        text = html.unescape(text)
        text = text_maker.handle(text)
        text = text.strip().capitalize()
        return text


