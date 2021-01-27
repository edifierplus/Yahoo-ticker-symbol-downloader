import math
from time import sleep

import requests

from ytd.compat import quote, text

user_agent = 'yahoo-ticker-symbol-downloader'
general_search_characters = 'abcdefghijklmnopqrstuvwxyz0123456789=.'
first_search_characters = 'abcdefghijklmnopqrstuvwxyz'


class SymbolDownloader:
    """Abstract class"""

    def __init__(self, type, starter=None):
        # All downloaded symbols are stored in a dict before exporting
        # This is to ensure no duplicate data
        self.symbols = dict()
        self.rsession = requests.Session()
        self.type = type
        self.queries = list()
        self.queries_set = set()
        self.idx = -1
        self.done = False
        self._start_queries(first_search_characters if starter is None else starter)

    def _start_queries(self, starter):
        for word in starter:
            if word not in self.queries_set:
                self.queries.append(word)
                self.queries_set.add(word)

    def _add_queries(self, prefix=''):
        # This method will add prefix plus one of general_search_characters to self.queries
        # The general_search_characters can be a letter, number, dot, or equals sign.
        for i in range(len(general_search_characters)):
            element = str(prefix) + str(general_search_characters[i])
            if element not in self.queries_set:  # Avoid having duplicates in list
                self.queries.append(element)
                self.queries_set.add(element)

    def _encodePara(self, params):
        encoded = ''
        for key, value in params.items():
            encoded += ';' + quote(key) + '=' + quote(text(value))
        return encoded

    def _fetch(self, insecure):
        params = {
            'searchTerm': self.queries[self.idx],
        }
        query_string = {
            'device': 'console',
            'returnMeta': 'true',
        }
        protocol = 'http' if insecure else 'https'
        req = requests.Request(
            'GET',
            protocol + '://finance.yahoo.com/_finance_doubledown/api/resource/searchassist' + self._encodePara(params),
            headers={'User-agent': user_agent},
            params=query_string
        )
        req = req.prepare()
        resp = self.rsession.send(req, timeout=(12, 12))
        resp.raise_for_status()

        return resp.json()

    def decodeSymbolsContainer(self, symbolsContainer):
        raise NotImplementedError("Function to extract symbols must be overwritten in subclass.")

    def getTotalQueries(self):
        return len(self.queries)

    def _nextQuery(self):
        if self.idx + 1 >= len(self.queries):
            self.idx = 0
        else:
            self.idx += 1

    def nextRequest(self, pbar, insecure=False, pandantic=False):
        self._nextQuery()
        success = False
        retryCount = 0
        json = None
        # Eponential back-off algorithm
        # to attempt 5 more times sleeping x, x^2, x^3, x^4, x^5 seconds respectively.
        maxRetries = 5
        firstSleep = 5  # seconds
        while not success:
            try:
                json = self._fetch(insecure)
                success = True
            except (requests.HTTPError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError) as ex:
                if retryCount < maxRetries:
                    retryCount += 1
                    sleepAmt = int(math.pow(firstSleep, retryCount))
                    pbar.write("Retry attempt: " + str(retryCount) + " of " + str(maxRetries) + "."
                               " Sleep period: " + str(sleepAmt) + " seconds.")
                    sleep(sleepAmt)
                else:
                    raise ex

        (symbols, count) = self.decodeSymbolsContainer(json)

        for symbol in symbols:
            self.symbols[symbol.ticker] = symbol

        # There is no pagination with this API.
        # If we receive 10 results, add another layer of queries by expending the query to narrow the search further.
        if(count == 10):
            self._add_queries(self.queries[self.idx])
        elif(count > 10):
            # This should never happen with this API, it always returns at most 10 items
            raise Exception("Funny things are happening: count "
                            + text(count)
                            + " > 10. "
                            + "Content:"
                            + "\n"
                            + repr(json))

        if self.idx + 1 >= len(self.queries):
            self.done = True
        else:
            self.done = False

        return symbols

    def isDone(self):
        return self.done

    def getCollectedSymbols(self):
        return self.symbols.values()

    def getRowHeader(self):
        return ["Ticker", "Name", "Exchange"]

    def getProgress(self):
        """Returns (# unique symbols, # processed queries, # total queries, current query)"""
        return (len(self.symbols), self.idx, len(self.queries), self.queries[self.idx])
