#!/usr/bin/env python

import requests
import sys
import bs4
import urllib2
import time
import random

from datetime import datetime as dt
from datetime import timedelta as td

MAX_DAYS = 365
PREF_DAYS = 100
DATE_FORMAT = '%d-%m-%Y'
VF_DATE = '1-1-1995'
VF_DATE_DT = dt.strptime(VF_DATE, DATE_FORMAT)

_INDICES_DICT = {
                    'NIFTY' : ('CNX Nifty', '03-07-1990'),
                    'JUNIOR' : ('CNX NIFTY JUNIOR', '04-10-1997'),
                    'CNX100' : ('CNX 100', '01-01-2003'),
                    'CNX200' : ('CNX 200', '01-01-2004'),
                    'CNX500' : ('CNX 500', '07-06-1999'),
                    'MIDCAP' : ('CNX MIDCAP', '01-01-2001'),
                    'SMALLCAP' : ('CNX SMALLCAP', '01-01-2004'),
                    'LIX15' : ('LIX 15', '01-01-2009'),
                    'LIX15MIDCAP' : ('LIX15 Midcap', '01-01-2009'),
                    'NIFTY_MIDCAP' : ('NIFTY MIDCAP 50', '01-01-2004'),
                    'CNXAUTO' : ('CNX AUTO', '01-01-2004'),
                    'BANKNIFTY' : ('BANK NIFTY', '01-01-2000'),
                    'CNXENERGY' : ('CNX ENERGY', '01-01-2001'),
                    'CNXFINANCE' : ('CNX FINANCE', '01-01-2004'),
                    'CNXFMCG' : ('CNX FMCG', '01-01-1996'),
                    'CNXIT' : ('CNX IT', '01-01-1996'),
                    'CNXMEDIA' : ('CNX MEDIA', '01-01-2006'),
                    'CNXMETAL' : ('CNX METAL', '01-01-2004'),
                    'CNXPHARMA' : ('CNX PHARMA', '01-01-2001'),
                    'CNXPSUBANK' : ('CNX PSU BANK', '01-01-2004'),
                    'CNXINFRA' : ('CNX INFRA', '01-01-2004'),
                    'CNXREALTY' : ('CNX REALTY', '01-01-2007'),
                    'CNXCOMMODITY' : ('CNX COMMODITIES', '01-01-2004'),
                    'CNXCONSUMPTION' : ('CNX CONSUMPTION', '01-01-2006'),
                    'VIX' : ('INDIA VIX', '01-01-2010'),

                }

def prepare():
    global _BASE_URL
    """Basic preparation for index download. Load a URL and any other setup
        required
    """
    base = 'http://nseindia.com/products/content/equities/indices/'\
            'historical_index_data.htm'
    response = requests.get(base)

def get_index(idx):
    """Returns an iterator over the rows of the data"""

    if idx not in _INDICES_DICT.keys():
        print "Index %s not found" % idx
        print "Possible Indices are: %s" % (", ".join(_INDICES_DICT.keys()))
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
            all_data.extend(r)

        time.sleep(random.randint(1,5))
        s = e2 + td(days=1)
        e2 = s + td(days=PREF_DAYS)
        if e2 > e:
            e2 = e

    return all_data


def _do_get_index(idx, start_dt, end_dt):
    prepare()
    print "getting data for " + idx + " : from : " + start_dt + " to : " + end_dt
    params = {'idxstr' : urllib2.quote(_INDICES_DICT[idx][0]),
                'from' : start_dt,
                'to'   : end_dt
             }
    u = 'http://nseindia.com/products/dynaContent/equities/indices/'\
        'historicalindices.jsp?indexType=%(idxstr)s&fromDate=%(from)s&toDate='\
        '%(to)s' % params
    response = requests.get(u)
    soup = bs4.BeautifulSoup(response.text)
    tbl = soup.find('table')
    if not tbl:
        return None
    vals = []
    rows = tbl.find_all('tr')
    if len(rows) <= 3: # Probably an error
        print rows[-1].text.strip()
        return None
    for i, row in enumerate(rows):
        if i <= 2:
            continue
        elif i == len(rows)-1:
            anchor = row.find('a')
            csv_link = anchor['href']
        else:
            vals.append(map(lambda x: x.strip(),
                            filter(lambda x: x, row.text.split('\n'))))
    ## Optionally get the CSV - this often gives 404, we've to find why!
    # The CSV downloading part is unreliable - so we are just downloading
    # 100 entries at a time
    #csvr = requests.get('http://nseindia.com/'+csv_link)
    #tries = 0
    #while csvr.status_code != 200 and tries < 2:
    #    csvr = requests.get('http://nseindia.com/'+csv_link)
    #    print "sleeping"
    #    time.sleep(1)
    #    tries += 1
    #print csvr.text
    return vals

def get_indices(indices):
    for idx in indices:
        idx_data = get_index(idx.upper())
        if not idx_data:
            continue
        for row in idx_data:
            print row
        #print idx_data

if __name__ == '__main__':
    if len(sys.argv) > 1:
        indices = sys.argv[1:]
    else:
        indices = _INDICES_DICT.keys()
    get_indices(indices)
