
import re 
import html
import datetime
import feedparser
import concurrent.futures


class Service:

    # RSS News Feeds
    feeds_dbname = 'RSS_FEEDS'
    symbols_dbname = 'STOCK_SYMBOLS'
    re_http_url = re.compile(r'^.*(https?://.+)$', re.IGNORECASE)

    executor = None
    logging = None
    idol = None

    def __init__(self, logging, threads, idol): 
        self.logging = logging 
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
        self.idol = idol 

    def index_feeds(self, filename='data/feeds'):
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
            content = f"{title}\n\n{summr}"

            idolSuggestions = self.idol.suggest_on_text(self.symbols_dbname, title, 'GREATER{1000.0}:SHAREOUTSTANDING_NUM', 20)
            self.logging.info(f"idolSuggestions: {len(idolSuggestions)} | {title}")
            if len(idolSuggestions) > 0:
                docsToIndex.append({
                    'reference': link,
                    'dbname': self.feeds_dbname,
                    'content': content,
                    'fields': {
                        'DATE': date,
                        'TITLE': title,
                        'SUMMARY': summr,
                    }  
                })
        response = { 'url': feed_url, 'count': len(docsToIndex), 'response': (self.idol.index_into_idol(docsToIndex) if len(docsToIndex) > 0 else 'n/a') }
        return response
