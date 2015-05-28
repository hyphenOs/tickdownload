"""
Download All stocks data from the NSE website. Here we make use of a stocks
CSV file - which has 'symbol and start date. We start from that date
(or 01-01-2002), whichever is later and download data up to today.
"""

import requests
from datetime import datetime as dt
from datetime import timedelta as td
import random
import time

import Queue
from collections import namedtuple

from nse_utils import get_all_stocks_list

GLOBAL_START_DATE = dt.strptime('01-01-2002', '%d-%m-%Y')

DATE_FORMAT = '%d-%m-%Y'
DAYS_AT_ONCE = 364 # max 365 allowed at a time

# Some error handling - max a stock can fail is MAX_RETRIES*MAX_ERRS times
MAX_RETRIES = 5
MAX_ERRS = 5

_failed = Queue.Queue()
sym_tuple = namedtuple('DownloadTuple', ['sym', 'start', 'end', 'errs'])

def get_data_for_security(symbol, sdate, edate=None):
    """ Get Data for a symbol with start date sdate. Optional edate can be
    specified. The format of the date is 'dd-Mon-YYYY'.
    """
    sdate = dt.strptime(sdate, '%d-%b-%Y')
    if sdate < GLOBAL_START_DATE:
        sdate = GLOBAL_START_DATE

    edate = dt.strptime(edate, '%d-%b-%Y') if edate is not None else dt.today()
    _do_get_data_for_security(symbol, sdate, edate)

def _do_get_data_for_security(symbol, sdate, edate, errs=0):
    """ The actual function that does all the work. Shouldn't be called
    directly from the outside. Does all the hardwork of downloading the data
    and storing it to CSV, upon error queues itself up for retry"""
    #print "Getting data For: ", symbol, " from ", sdate

    symbol = symbol.replace('&', '%26')
    e = sdate + td(days=DAYS_AT_ONCE)

    if e > edate:
        e = edate

    headers = {
        'Accept' : '*/*',
        'Accept-Encoding' : 'gzip, deflate, sdch',
        'Accept-Language' : 'en-US,en;q=0.8',
        'Connection' : 'keep-alive',
        'User-Agent' : 'Mozilla/5.0 (Windows NT 6.3; WOW64) '\
                        'AppleWebKit/537.36 (KHTML, like Gecko)'\
                        'Chrome/43.0.2357.65 Safari/537.36'
    }
    symbol = symbol.lower()
    while sdate < e:
        s_ = sdate.strftime(DATE_FORMAT)
        e_ = e.strftime(DATE_FORMAT)

        url = 'http://nseindia.com/products/dynaContent/common/'\
                'productsSymbolMapping.jsp?symbol=%s&'\
                'segmentLink=3&symbolCount=2&series=ALL&dateRange=+&'\
                'fromDate=%s&toDate=%s&dataType=PRICEVOLUMEDELIVERABLE' % \
                (symbol, s_, e_)
        #print "Getting...", url
        r = requests.get(url, allow_redirects=False,
                            headers=headers)
        print r.status_code
        if r.status_code == 302:
            r = requests.get(r.headers.get('location'))
            cookies = requests.utils.dict_from_cookiejar(r.cookies)
        if r.status_code == 200 :
            cookies = requests.utils.dict_from_cookiejar(r.cookies)
            time.sleep(2)
            fname = '%s-TO-%s%sALLN.csv' % (s_, e_, symbol.upper())
            url2 = 'http://www.nseindia.com/content/'\
                    'equities/scripvol/datafiles/' + fname
            r = requests.get(url2, headers=headers, cookies=cookies)
            retries = 0
            while r.status_code != 200:
                r = requests.get(url2, headers=headers, cookies=cookies)
                retries += 1
                time.sleep(1)
                if retries >= MAX_RETRIES:
                    break
            if r.status_code == 200:
                f = open(fname, 'w+')
                f.write(r.text)
                f.close()
            else:
                print "*****Error in downloading for : ", symbol, s_, e_
                _failed.put(sym_tuple(symbol, sdate, e, errs+1))

        sdate = e + td(days=1)
        e = sdate + td(days=DAYS_AT_ONCE)
        if e > edate:
            e = edate


def get_all_stocks_data(start_index=None, count=-1):
    """ start_index and count can be used for downloading only a subset
    of data @start_index for @count number of stocks"""

    i = 0
    for sym, name, sdate in get_all_stocks_list(start_index, count):
        time.sleep(random.randint(1,5))
        get_data_for_security(sym, sdate)

    # We downloaded. There might be some errors, we try this again
    global _failed
    while not _failed.empty():
        s = _failed.get()
        if (s.errs > MAX_ERRS):
            print "Too many errors :", s.errs, \
                    "Hopelessly giving up on ", s.sym, s.start
            continue
        _do_get_data_for_security(s.sym, s.start, s.end, s.errs)


if __name__ == '__main__':
    get_all_stocks_data(1,1)
