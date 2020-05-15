from __future__ import unicode_literals, print_function

import plac
import json
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
config = {}
#executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
with open('config.json') as json_configfile:
    config = json.load(json_configfile)
logging.basicConfig(format='%(asctime)s (%(threadName)s) %(levelname)s - %(message)s', level=logging.INFO, handlers=[logging.FileHandler(config.get('logfile', logfile), 'w', 'utf-8')])

def main():
    logging.info("==============> Starting service... ")
    logging.info(config)

    # IDOL 
    idolService = idol.Service(logging, config)
    
    # INDEX RSS FEEDS
    #'''
    rssService = rss.Service(logging, config, idolService)
    results = rssService.index_feeds('data/feeds.rss')
    for _r in results:
        logging.info(_r)
    #'''
    
    # INDEX STOCK SYMBOLS
    '''
    stockService = stock.Service(logging, config, idolService)
    #exchangeCodes = stockService.list_exchange_codes()
    #stockService.index_stocks_symbols(exchangeCodes)
    stockService.index_stocks_symbols(['US'])
    '''

    # NLP
    '''
    nlpService = nlp.Service(logging, config, idolService)
    idolQuery = {
        'DatabaseMatch' : 'RSS_FEEDS',
        'MinScore' : 30,
        'MaxResults': 50,
        'Text' : 'Apple'
    }
    nlpService.export_training_json(idolQuery)
    nlpService.doccano_login('admin', 'password')
    nlpService.import_training_json('deep-web')
    '''


    #logging.info(resp)
    







    #train_model_sentiment('en_core_web_sm')
    #train_model_sentiment('pt_core_news_sm')
    #train_model_ner('en_core_web_sm')
    #train_model_ner('pt_core_news_sm')
    #index_feeds()
    #stock.index_stock_symbols('BR')




if __name__ == "__main__":
    plac.call(main)