"""
Common utilities for getting stocks, indices data from NSE.
"""

import requests

from collections import namedtuple

stock_base_info = namedtuple('StockBasicInfo', ['symbol', 'name', 'listing_date'])

ALL_STOCKS_CSV_URL = 'http://nseindia.com/corporates/datafiles/'\
                        'LDE_EQUITIES_MORE_THAN_5_YEARS.csv'

def get_all_stocks_list(start=None, count=-1):
    """ Returns a generator object of all stocks as a
        namedtuple(symbol, name, listing_date)"""

    try:
        start = int(start) or 0
        count = int(count) or -1
    except ValueError: # Make sure both start and count can be 'int'ed
        raise

    r = requests.get(ALL_STOCKS_CSV_URL)
    if r.ok:
        i = 0
        for line in r.text.split("\n"):
            line = line.split(",")
            if len(line) < 8:
                print "Line: ", line, " Not in correct format"
                continue
            symbol = line[0].strip('"')
            if symbol.lower == 'symbol':
                continue
            if i < start:
                i += 1
                continue
            if count > 0 and i >= start+count:
               raise StopIteration
            name = line[2].strip('"')
            listing_date = line[3].strip('"')
            a = stock_base_info(symbol, name, listing_date)
            i += 1
            yield a
    else:
        raise StopIteration

if __name__ == '__main__':
    for x in get_all_stocks_list(start=10, count=5):
        print x


