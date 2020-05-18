
import re 
import time
import uuid 
import html
import random
import datetime
import html2text
import feedparser
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict
filters_fieldprefix = 'FILTERINDEX'

class Service:

    re_http_url = re.compile(r'^.*(https?://.+)$', re.IGNORECASE)    
    text_maker = None
    executor = None
    logging = None
    config = None
    idol = None

    def __init__(self, logging, config, idol): 
        self.logging = logging 
        self.config = config
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2), thread_name_prefix='RssPool')
        self.idol = idol 
        self.text_maker = html2text.HTML2Text()
        self.text_maker.ignore_links = True
        self.text_maker.ignore_images = True
        
    def index_feeds(self, filename=None):
        if filename == None: filename = self.config.get('feeds', 'data/feeds')
        feeds_file = open(filename, 'r') 
        Lines = feeds_file.readlines() 
        feeds_urls = []
        for _l in Lines: 
            _url = _l.strip()
            if self.re_http_url.match(_url):
                feeds_urls.append(_url)
        feeds_file.close()

        random.shuffle(feeds_urls)
        feeds_threads = []
        for _url in feeds_urls:
            feeds_threads.append((_url, self.executor.submit(self.get_feed_from_url, _url)))

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
            time.sleep(1)

        responses = []
        for _t in index_threads:
            responses.append(_t.result()) 
        return responses

 
    @retry(wait_fixed=10000, stop_max_delay=30000)
    def get_feed_from_url(self, feed_url):
        self.logging.info(f"crawling: {feed_url}")
        return feedparser.parse(feed_url)

    def index_feed(self, feed_url, feed):
        try:
            return self._index_feed(feed_url, feed)
        except Exception as error:
            self.logging.error(f"RSS_URL: {feed_url} | {str(error)}")
            return { 'url': feed_url, 'error': str(error) }

    def _index_feed(self, feed_url, feed):      
        self.logging.info(f"processing: {feed_url}")
        docsToIndex = []
        #feed = self.get_feed_from_url(feed_url)
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
                
                self.logging.debug(f"idol_filter: {idolHits} | {feed_url}")
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
            self.idol.index_into_idol(docsToIndex, self.config.get('database'))

        return { 'url': feed_url, 'total': len(feed.entries), 'indexed': len(docsToIndex) }

    def cleanText(self, text):
        text = html.unescape(text)
        text = self.text_maker.handle(text)
        text = text.strip().capitalize()
        return text


