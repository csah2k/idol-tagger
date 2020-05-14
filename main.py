from __future__ import unicode_literals, print_function

import plac
import random
import logging
import concurrent.futures

import services.idol as idol
import services.stock as stock
import services.rss as rss

# TODO - add twitter source
# https://python-twitter.readthedocs.io/en/latest/getting_started.html


logfile='main.log'

#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=logging.INFO, handlers=[logging.FileHandler(logfile, 'w', 'utf-8')])

def main():
    logging.info("Starting deep crawler...")

    idolService = idol.Service(logging, 8)
    #stockService = stock.Service(logging, 2, idolService)
    
    rssService = rss.Service(logging, 4, idolService)
    resp = rssService.index_feeds('data/feeds.rss')
    logging.info(resp)


    #exchangeCodes = stockService.list_exchange_codes()
    #stockService.index_stocks_symbols(exchangeCodes)

    #train_model_sentiment('en_core_web_sm')
    #train_model_sentiment('pt_core_news_sm')
    #train_model_ner('en_core_web_sm')
    #train_model_ner('pt_core_news_sm')
    #index_feeds()
    #stock.index_stock_symbols('BR')




if __name__ == "__main__":
    plac.call(main)