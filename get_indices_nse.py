#!/usr/bin/env python
#
# Refer to LICENSE file and README file for licensing information.
#

import os
from utils import get_logger
module_logger = get_logger(os.path.basename(__file__))

import requests
import sys
import bs4
import urllib2
import time
import random

from datetime import datetime as dt
from datetime import timedelta as td

from sqlalchemy_wrapper import create_or_get_nse_indices_hist_data
from sqlalchemy_wrapper import execute_many

MAX_DAYS = 365
PREF_DAYS = 100
DATE_FORMAT = '%d-%m-%Y'
VF_DATE = '1-1-1995'
VF_DATE_DT = dt.strptime(VF_DATE, DATE_FORMAT)

_INDICES_DICT = {
                    'NIFTY' : ('NIFTY 50', '03-07-1990'),
                    'JUNIOR' : ('NIFTY NEXT 50', '24-12-1996'),
                    'CNX100' : ('NIFTY 100', '01-12-2005'),
                    'CNX200' : ('NIFTY 200', '03-10-2011'),
                    'CNX500' : ('NIFTY 500', '07-06-1999'),
                    #'MIDCAP' : ('CNX MIDCAP', '01-01-2001'),
                    #'SMALLCAP' : ('CNX SMALLCAP', '01-01-2004'),
                    #'LIX15' : ('LIX 15', '01-01-2009'),
                    #'LIX15MIDCAP' : ('LIX15 Midcap', '01-01-2009'),
                    #'NIFTY_MIDCAP2' : ('NIFTY MIDCAP 150', '01-01-2004'),
                    'NIFTY_MIDCAP' : ('NIFTY MIDCAP 50', '25-09-2007'),
                    #'CNXAUTO' : ('CNX AUTO', '01-01-2004'),
                    #'BANKNIFTY' : ('BANK NIFTY', '01-01-2000'),
                    #'CNXENERGY' : ('CNX ENERGY', '01-01-2001'),
                    #'CNXFINANCE' : ('CNX FINANCE', '01-01-2004'),
                    #'CNXFMCG' : ('CNX FMCG', '01-01-1996'),
                    #'CNXIT' : ('CNX IT', '01-01-1996'),
                    #'CNXMEDIA' : ('CNX MEDIA', '01-01-2006'),
                    #'CNXMETAL' : ('CNX METAL', '01-01-2004'),
                    #'CNXPHARMA' : ('CNX PHARMA', '01-01-2001'),
                    #'CNXPSUBANK' : ('CNX PSU BANK', '01-01-2004'),
                    #'CNXINFRA' : ('CNX INFRA', '01-01-2004'),
                    #'CNXREALTY' : ('CNX REALTY', '01-01-2007'),
                    #'CNXCOMMODITY' : ('CNX COMMODITIES', '01-01-2004'),
                    #'CNXCONSUMPTION' : ('CNX CONSUMPTION', '01-01-2006'),
                    #'VIX' : ('INDIA VIX', '01-01-2010'),

                }
def prepare():
    global _BASE_URL
    """Basic preparation for index download. Load a URL and any other setup
        required
    """
    base = 'http://nseindia.com/products/content/equities/indices/'\
            'historical_index_data.htm'
    response = requests.get(base)

def download_and_save_index(idx):
    """
    Returns an iterator over the rows of the data

    The way this works is - we download data for 100 days at a time - something
    that fits in the table and then read that table using BS4. Then collect all
    such data and return back.
    """

    if idx not in _INDICES_DICT.keys():
        module_logger.error("Index %s not found or not supported yet." % idx)
        module_logger.error("supported Indices are: %s" % \
                                    (", ".join(_INDICES_DICT.keys())))
        return None

    start_dt = _INDICES_DICT[idx][1]
    s = dt.strptime(start_dt, DATE_FORMAT)
    e = dt.now()
    e2 = s + td(days=PREF_DAYS)
    delta = e - s
    all_data = []

    while e > s:
        e_ = e2.strftime(DATE_FORMAT)
        s_ = s.strftime(DATE_FORMAT)
        r = _do_get_index(idx, s_, e_)
        if r:
            module_logger.debug("Downloaded %d records" % len(r))
            all_data.extend(r)
        else:
            module_logger.info("Unable to download some records for"
                                "%s (%s-%s)" % (idx, s_, e_))

        time.sleep(random.randint(1,5))
        s = e2 + td(days=1)
        e2 = s + td(days=PREF_DAYS)
        if e2 > e:
            e2 = e

    tbl = create_or_get_nse_indices_hist_data()

    insert_statements = []
    for row in all_data:
        d = dt.date(dt.strptime(row[0].strip(), '%d-%b-%Y'))
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])

        insert_st = tbl.insert().values(symbol=idx,
                                        date=d,
                                        open=o,
                                        high=h,
                                        low=l,
                                        close=c)
        insert_statements.append(insert_st)

    execute_many(insert_statements)

    return all_data

def _do_get_index(idx, start_dt, end_dt):
    prepare()
    module_logger.info("getting data for %s : from : %s to : %s" % \
                        (idx, start_dt, end_dt))

    params = {'idxstr' : urllib2.quote(_INDICES_DICT[idx][0]),
                'from' : start_dt,
                'to'   : end_dt
             }
    try:
        u = 'http://nseindia.com/products/dynaContent/equities/indices/'\
            'historicalindices.jsp?indexType=%(idxstr)s&'\
            'fromDate=%(from)s&toDate=%(to)s' % params
        response = requests.get(u)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        tbl = soup.find('table')
        if not tbl:
            return None
        vals = []
        rows = tbl.find_all('tr')
        if len(rows) <= 3: # Probably an error
            module_logger.debug("fewer rows, possibly an error. %s" % \
                                rows[-1].text.strip())
            return None
    except requests.RequestsException as e:
        module_logger.exception(e)
        return None

    for i, row in enumerate(rows):
        if i <= 2:
            continue
        elif i == len(rows)-1:
            pass # previously this used to give href, now we ignore this
            # anchor = row.find('a')
            # csv_link = anchor['href']
        else:
            vals.append(map(lambda x: x.strip(),
                            filter(lambda x: x, row.text.split('\n'))))
    ## Optionally get the CSV - this often gives 404, we've to find why!
    # The CSV downloading part is unreliable - so we are just downloading
    # 100 entries at a time
    return vals

def get_indices(indices):
    for idx in indices:
        module_logger.info("Downloading data for %s." % idx.upper())
        download_and_save_index(idx.upper())

if __name__ == '__main__':
    if len(sys.argv) > 1:
        indices = sys.argv[1:]
    else:
        indices = _INDICES_DICT.keys()
    get_indices(indices)
