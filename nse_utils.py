#
# Refer to LICENSE file and README file for licensing information.
#
"""
Common utilities for getting stocks, indices data from NSE.
"""

import os
from utils import get_logger
module_logger = get_logger(os.path.basename(__file__))

import requests

from collections import namedtuple

ScripOHLCVD = namedtuple('ScripOHLCVD',
                            ['open', 'high', 'low', 'close', 'volume', 'deliv'])

ScripBaseinfoNSE = namedtuple('ScripBaseinfoNSE',
                                    ['symbol', 'name', 'listing_date', 'isin'])

ALL_STOCKS_CSV_URL = 'http://nseindia.com/corporates/datafiles/'\
                        'LDE_EQUITIES_MORE_THAN_5_YEARS.csv'

def nse_get_all_stocks_list(start=None, count=-1):
    """ Returns a generator object of all stocks as a
        namedtuple(symbol, name, listing_date, isin)"""

    start = start or 0
    try:
        start = int(start) or 0
        count = int(count) or -1
    except ValueError: # Make sure both start and count can be 'int'ed
        raise

    r = requests.get(ALL_STOCKS_CSV_URL)
    module_logger.info("GET: %s" % ALL_STOCKS_CSV_URl)
    if r.ok:
        i = 0
        for line in r.text.split("\n"):
            line = line.split(",")
            if len(line) < 8:
                module_logger.info("Unhandled line: %s" % line)
                continue
            symbol = line[0].strip('"')
            if symbol.lower().strip() == 'symbol':
                continue
            if i < start:
                i += 1
                continue
            if count > 0 and i >= start+count:
               raise StopIteration
            name = line[2].strip('"')
            listing_date = line[3].strip('"')
            isin = line[1].strip('"')
            a = ScripBaseinfoNSE(symbol, name, listing_date, isin)
            module_logger.debug("ScripBaseInfoNSE: %s" % str(a))
            i += 1
            yield a
    else:
        module_logger.error("GET: %s(%d)" % (ALL_STOCKS_CSV_URl, x.status_code))
        raise StopIteration

def nse_get_name_change_tuples():
    """Returns a list of name changes as a tuples, the most current name
    is the last name in the tuple."""

    r = requests.get('http://nseindia.com/content/equities/symbolchange.csv')
    if not r.ok:
        return []

    name_tuples = []
    changed_names = []
    for line in r.text.split('\n'):
        x = line.split(',')
        if len(x) < 3:
            continue
        prev, cur = x[1].strip().upper(), x[2].strip().upper()
        if prev in changed_names:
            tup = filter(lambda x: x[len(x)-1] == prev, name_tuples)
            name_tuples.remove(tup[0])
            name_tuples.append((tup[0] + (cur,)))
        else:
            name_tuples.append((prev, cur,))
        module_logger.debug("name_change_tuple: %s" % str((prev, cur,)))
        changed_names.append(cur)

    return name_tuples

if __name__ == '__main__':
    nse_get_name_change_tuples()
    for x in nse_get_all_stocks_list(count=-1):
        print x
