

import re 
import html
import logging
import requests
import datetime
import concurrent.futures
from retrying import retry
from requests.structures import CaseInsensitiveDict
filters_fieldprefix = 'FILTERINDEX'

# https://finnhub.io/docs/api
class Service:
    
    executor = None
    logging = None
    config = None
    idol = None

    def __init__(self, logging, config, idol): 
        self.logging = logging
        self.config = config.copy()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2), thread_name_prefix='StockPool')
        self.idol = idol 

    @retry(wait_fixed=10000, stop_max_delay=70000)
    def get_symbol_profile(self, symbol):
        url = f"{self.config.get('url')}/stock/profile2"
        query = {'token':self.config.get('api'), 'symbol':symbol}
        headers = {'X-Finnhub-Secret': self.config.get('key')}
        response = requests.request('GET', url, headers=headers, params=query)
        try:
            return response.json()
        except Exception as error:
            self.logging.error(f"{response} - {str(error)}")
            raise error

    def list_exchange_codes(self):
        return self.executor.submit(self._list_exchange_codes).result()

    def _list_exchange_codes(self):
        return list(set([_e.get('code', None) for _e in self.get_stock_exchanges() if _e.get('code', None) != None]))   

    @retry(wait_fixed=10000, stop_max_delay=90000)
    def get_stock_exchanges(self):
        self.logging.info(f"Listing stock exchanges...")
        url = f"{self.config.get('url')}/stock/exchange"
        query = {'token':self.config.get('api')}
        headers = {'X-Finnhub-Secret': self.config.get('key')}
        response = requests.request('GET', url, headers=headers, params=query)
        try:
            return response.json()
        except Exception as error:
            self.logging.error(f"{response} - {str(error)}")
            raise error

    def index_stocks_symbols(self, exchanges=['US']):
        self.logging.info(f"==== Starting ====>  STOCK indextask '{self.config.get('name')}'")
        threads = []
        for _e in exchanges: 
            threads.append(self.executor.submit(self.index_stock_symbols, _e))
        responses = []
        for _t in threads:
            _r = _t.result()
            responses.append(_r)   
        self.logging.info(f"STOCK indextask '{self.config.get('name')}' completed")
        return responses
            
    @retry(wait_fixed=10000, stop_max_delay=90000)
    def index_stock_symbols(self, exchange):
        exchange = exchange.strip().upper()
        self.logging.info(f"Indexing {exchange} stock symbols...")
        docsToIndex = []
        date = datetime.datetime.now().isoformat()
        url = f"{self.config.get('url')}/stock/symbol"
        query = {'token':self.config.get('api'), 'exchange':exchange}
        headers = {'X-Finnhub-Secret': self.config.get('key')}
        response = requests.request('GET', url, headers=headers, params=query)
        try:
            response = response.json()
            for _s in response:
                _p = {}
                try:
                    _p = self.get_symbol_profile(_s.get('symbol'))
                except Exception as error:
                    self.logging.error(f"{error}")
                self.logging.info(_p)

                shares = float(_p.get('shareOutstanding', 0))
                capitl = float(_p.get('marketCapitalization', 0))
                price =  (capitl / shares) if capitl > 0 and shares > 0 else 0

                # TODO use the idol webconnector to retrieve contents from the 'weburl' then index its bests concepts
                docsToIndex.append({
                    'reference': f"{exchange}_{_s.get('symbol')}",
                    'dbname': self.config.get('database'),
                    'drecontent': _p.get('name', _s.get('description', _s.get('symbol'))),
                    'fields': [
                        ('LANGUAGE', 'GENERALUTF8'),
                        ('DATE', date),
                        ('TITLE', f"{_p.get('name', _s.get('description'))} ({_s.get('symbol')})"),
                        ('DISPLAYSYMBOL',  _s.get('displaySymbol')),
                        ('SYMBOL_MATCH',  _s.get('symbol')),
                        ('DESCRIPTION',  _s.get('description')),
                        ('EXCHANGE_PARAM',  exchange),
                        ('EXCHANGE',  _p.get('exchange', '')),
                        ('IPO_DATE', _p.get('ipo', '')),
                        ('URL', _p.get('weburl', '')),
                        ('LOGO', _p.get('logo', '')),
                        ('COUNTRY_PARAM', _p.get('country', '')),
                        ('CURRENCY_PARAM', _p.get('currency', '')),
                        ('FINNHUBINDUSTRY_PARAM', _p.get('finnhubIndustry', '')),
                        ('SHAREOUTSTANDING_NUM', shares),
                        ('MARKETCAPITALIZATION_NUM', capitl),
                        ('SINGLESHAREPRICE_NUM', price)
                    ]  
                })
        except Exception as error:
            self.logging.error(f"{response} - {str(error)}")
            raise error

        query = {
            'DREDbName': self.config.get('database'),
            'KillDuplicates': 'REFERENCE',
            'CreateDatabase': True,
            'KeepExisting': False,
            'Priority': 0
        }
        self.idol.index_into_idol(docsToIndex, query)
        return { 'url': url, 'total': len(docsToIndex), 'indexed': len(docsToIndex) }
