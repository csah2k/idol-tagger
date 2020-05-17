
import re 
import html
import datetime
import html2text
import feedparser
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict

class Service:

    re_http_url = re.compile(r'^.*(https?://.+)$', re.IGNORECASE)
    #re_non_word = re.compile(r'[\W\"\'\)\(\]\[+=-\*\?]', re.IGNORECASE)
    filter_field_prefix = 'FILTERINDEX'
    text_maker = None
    executor = None
    logging = None
    config = None
    idol = None

    def __init__(self, logging, config, idol): 
        self.logging = logging 
        self.config = config
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2))
        self.idol = idol 
        self.text_maker = html2text.HTML2Text()
        self.text_maker.ignore_links = True
        self.text_maker.ignore_images = True
        
    def index_feeds(self, filename=None):
        if filename == None: filename = self.config.get('feeds', 'data/feeds')
        feeds_file = open(filename, 'r') 
        Lines = feeds_file.readlines() 
        feeds_threads = []
        for _l in Lines: 
            url = _l.strip()
            if self.re_http_url.match(url):
                feeds_threads.append(self.executor.submit(self.index_feed, url))
        feeds_file.close()
        responses = []
        for _t in feeds_threads:
            _r = _t.result()
            responses.append(_r) 
        return responses

    def index_feed(self, feed_url):
        try:
            return self._index_feed(feed_url)
        except Exception as error:
            self.logging.error(f"RSS_URL: {feed_url} | {str(error)}")
            return { 'url': feed_url, 'error': str(error) }

    @retry(wait_fixed=10000, stop_max_delay=30000)
    def get_feed_from_url(self, feed_url):
        return feedparser.parse(feed_url)

    def _index_feed(self, feed_url):      
        self.logging.info(f"INDEXING: {feed_url}")
        docsToIndex = []
        feed = get_feed_from_url(feed_url)
        for _e in feed.entries:
            link = None
            try:
                link = _e.get('link', _e.get('href', _e.get('url', _e.get('links', [{'href':feed_url}])[0].get('href', feed_url) )))
                if self.re_http_url.match(link): link = self.re_http_url.search(link).group(1)
                date = _e.get('published', _e.get('timestamp', _e.get('date')))
                title = self.cleanText(_e.get('title', _e.get('titulo', _e.get('headline',''))))
                summr = self.cleanText(_e.get('summary', _e.get('description', _e.get('text',''))))
                content = f"{title}\n{summr}"
                lang_info= self.idol.detect_language(content)

                idolHits = []
                for _query in self.config.get('filters'):
                    idolQuery = _query.copy()
                    idolQuery['Text'] = content
                    idolQuery['SingleMatch'] = True
                    idolQuery['IgnoreSpecials'] = True
                    idolHits += self.idol.query(idolQuery)

                if link == feed_url:
                    self.logging.warn(f"{_e}")  
                
                self.logging.debug(f"idol_filter: {idolHits} | {feed_url}")
                if len(idolHits) > 0:
                    docsToIndex.append({
                        'reference': link,
                        'drecontent': content,
                        'fields': [
                            ('LANGUAGE', lang_info.get('language')+lang_info.get('encoding')),
                            ('DATE', date),
                            ('TITLE', title),
                            ('SUMMARY', summr) ]
                        + [(f'{self.filter_field_prefix}_PARAM', _hit.get('database')) for _hit in idolHits] 
                        + [(f'{self.filter_field_prefix}_LINKS', _hit.get('links')) for _hit in idolHits]
                        + [(f'{self.filter_field_prefix}_REFS', _hit.get('reference')) for _hit in idolHits]
                    })
            except Exception as error:
                self.logging.error(f"ENTRY_URL: {link} | {str(error)}")

        response = { 'url': feed_url, 'count': len(docsToIndex), 'response': (self.idol.index_into_idol(docsToIndex, self.config.get('database')) if len(docsToIndex) > 0 else 'n/a') }
        return response

    def cleanText(self, text):
        text = html.unescape(text)
        text = self.text_maker.handle(text)
        return text


