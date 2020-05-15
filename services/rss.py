
import re 
import html
import datetime
import feedparser
import concurrent.futures


class Service:

    re_http_url = re.compile(r'^.*(https?://.+)$', re.IGNORECASE)

    executor = None
    logging = None
    config = None
    idol = None

    def __init__(self, logging, config, idol): 
        self.logging = logging 
        self.config = config.get('rss')
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2))
        self.idol = idol 
        
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
        self.logging.info(f"INDEXING: {feed_url}")
        docsToIndex = []
        feed = feedparser.parse(feed_url)
        for _e in feed.entries:
            link = _e.get('link', _e.get('href', _e.get('url', _e.get('links', [{'href':feed_url}])[0].get('href', feed_url) )))
            if self.re_http_url.match(link): link = self.re_http_url.search(link).group(1)
            date = _e.get('published', _e.get('timestamp', _e.get('date')))
            title = html.unescape(_e.get('title', _e.get('titulo', _e.get('headline',''))))
            summr = html.unescape(_e.get('summary', _e.get('description', _e.get('text',''))))
            content = f"{title}\n{summr}"
            lang_info= self.idol.detect_language(content)

            idolSuggestions = []
            for _query in self.config.get('suggest'):
                idolQuery = _query.copy()
                idolQuery['Text'] = title
                idolSuggestions += self.idol.suggest_on_text(idolQuery)
            idolSuggestions = [_s.get('database') for _s in idolSuggestions]
            
            self.logging.info(f"idolSuggest: {idolSuggestions} | {title}")
            if len(idolSuggestions) > 0:
                docsToIndex.append({
                    'reference': link,
                    'dbname': self.config.get('database'),
                    'content': content,
                    'fields': [
                        ('LANGUAGE', lang_info.get('language')+lang_info.get('encoding')),
                        ('DATE', date),
                        ('TITLE', title),
                        ('SUMMARY', summr),
                    ] + [('SUGGEST_PARAM', _db) for _db in idolSuggestions]
                })
        response = { 'url': feed_url, 'count': len(docsToIndex), 'response': (self.idol.index_into_idol(docsToIndex) if len(docsToIndex) > 0 else 'n/a') }
        return response
