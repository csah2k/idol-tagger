

import re 
import html
import requests
import datetime
import concurrent.futures
from retrying import retry

class Service:
    # https://finnhub.io/docs/api
    stocks_url = 'https://finnhub.io/api/v1'
    stocks_key = 'bqu04u7rh5rb3gqqfb1g'
    stocks_api = 'bqu00jvrh5rb3gqqf9q0'
    symbols_dbname = 'STOCK_SYMBOLS'

    executor = None
    logging = None
    idol = None

    def __init__(self, logging, threads, idol): 
        self.logging = logging
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
        self.idol = idol 

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=600000)
    def get_symbol_profile(self, symbol):
        url = f'{self.stocks_url}/stock/profile2'
        query = {'token':self.stocks_api, 'symbol':symbol}
        headers = {'X-Finnhub-Secret': self.stocks_key}
        response = requests.request('GET', url, headers=headers, params=query)
        try:
            return response.json()
        except Exception as error:
            self.logging.error(f"{response} - {response.text} - {str(error)}")
            raise error

    def list_exchange_codes(self):
        return self.executor.submit(self.list_exchange_codes_sync).result()

    def list_exchange_codes_sync(self):
        return list(set([_e.get('code', None) for _e in self.get_stock_exchanges() if _e.get('code', None) != None]))   

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=600000)
    def get_stock_exchanges(self):
        self.logging.info(f"Listing stock exchanges...")
        url = f'{self.stocks_url}/stock/exchange'
        query = {'token':self.stocks_api}
        headers = {'X-Finnhub-Secret': self.stocks_key}
        response = requests.request('GET', url, headers=headers, params=query)
        try:
            return response.json()
        except Exception as error:
            self.logging.error(f"{response} - {response.text} - {str(error)}")
            raise error

    def index_stocks_symbols(self, exchanges=['US']):
        threads = []
        for _e in exchanges: 
            threads.append(self.executor.submit(self.index_stock_symbols, _e))
        responses = []
        for _t in threads:
            _r = _t.result()
            responses.append(_r)   
        return responses
            
    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=600000)
    def index_stock_symbols(self, exchange):
        exchange = exchange.strip().upper()
        self.logging.info(f"Indexing {exchange} stock symbols...")
        docsToIndex = []
        date = datetime.datetime.now().isoformat()
        url = f'{self.stocks_url}/stock/symbol'
        query = {'token':self.stocks_api, 'exchange':exchange}
        headers = {'X-Finnhub-Secret': self.stocks_key}
        response = requests.request('GET', url, headers=headers, params=query)
        try:
            response = response.json()
            for _s in response:
                _p = self.get_symbol_profile(_s.get('symbol'))
                self.logging.info(_p)

                shares = float(_p.get('shareOutstanding', 0))
                capitl = float(_p.get('marketCapitalization', 0))
                price =  (capitl / shares) if capitl > 0 and shares > 0 else 0

                docsToIndex.append({
                    'reference': f"{exchange}_{_s.get('symbol')}",
                    'dbname': self.symbols_dbname,
                    'content': _p.get('name', _s.get('description', _s.get('symbol'))),
                    'fields': {
                        'DATE': date,
                        'TITLE': f"{_p.get('name', _s.get('description'))} ({_s.get('symbol')})",
                        'DISPLAYSYMBOL':  _s.get('displaySymbol'),
                        'SYMBOL_MATCH':  _s.get('symbol'),
                        'DESCRIPTION':  _s.get('description'),
                        'EXCHANGE_PARAM':  exchange,
                        'EXCHANGE':  _p.get('exchange', ''),
                        'IPO_DATE': _p.get('ipo', ''),
                        'URL': _p.get('weburl', ''),
                        'LOGO': _p.get('logo', ''),
                        'COUNTRY_PARAM': _p.get('country', ''),
                        'CURRENCY_PARAM': _p.get('currency', ''),
                        'FINNHUBINDUSTRY_PARAM': _p.get('finnhubIndustry', ''),
                        'SHAREOUTSTANDING_NUM': shares,
                        'MARKETCAPITALIZATION_NUM': capitl,
                        'SINGLESHAREPRICE_NUM': price
                    }  
                })
        except Exception as error:
            self.logging.error(f"{response} - {response.text} - {str(error)}")
            raise error
        
        response = { 'url': url, 'exchange': exchange, 'count': len(docsToIndex), 'response': (self.idol.index_into_idol(docsToIndex) if len(docsToIndex) > 0 else '') }
        self.logging.info(response)
        return response
