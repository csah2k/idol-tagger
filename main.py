from __future__ import unicode_literals, print_function

import plac
import random
import logging
import concurrent.futures

import services.idol as idol
import services.stock as stock
import services.rss as rss
import services.nlp as nlp

# TODO - add twitter source
# https://python-twitter.readthedocs.io/en/latest/getting_started.html


logfile='main.log'

#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=logging.INFO, handlers=[logging.FileHandler(logfile, 'w', 'utf-8')])

def main():
    logging.info("Starting deep crawler...")

    idolService = idol.Service(logging, 8)
    
    # INDEX RSS FEEDS
    #rssService = rss.Service(logging, 4, idolService)
    #rssService.index_feeds('data/feeds.rss')
    
    # INDEX STOCK SYMBOLS
    #stockService = stock.Service(logging, 2, idolService)
    #exchangeCodes = stockService.list_exchange_codes()
    #stockService.index_stocks_symbols(exchangeCodes)

    # NLP
    nlpService = nlp.Service(logging, 4, idolService)
    nlpService.export_training_sentiment_jsonl('Apple')
    #logging.info(resp)
    







    #train_model_sentiment('en_core_web_sm')
    #train_model_sentiment('pt_core_news_sm')
    #train_model_ner('en_core_web_sm')
    #train_model_ner('pt_core_news_sm')
    #index_feeds()
    #stock.index_stock_symbols('BR')




if __name__ == "__main__":
    plac.call(main)