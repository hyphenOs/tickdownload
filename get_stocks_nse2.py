"""
Uses daily bhavcopy to download historical data for all stocks.
One challenge is, bhavcopy has the symbol name for that day. In case of NSE,
the symbol names change and that's a problem, so we need a way to track
changes in symbol names as well.

All this data is stored in an SQLite database as we download, as some of the
updates (eg. update data corresponding to a symbol with a new name are easier
to handle in SQLite than dealing with files).
"""


import requests
import sys
from datetime import datetime as dt
from datetime import timedelta as td
if sys.version_info.major < 3:
    from StringIO import StringIO as bio
    from itertools import izip
else:
    from io import BytesIO as bio
    izip = zip

from zipfile import ZipFile

from nse_utils import get_name_change_tuples, scrip_ohlcvd_info

import sqlite3

# SQLite database for stocks data. There are following tables
# bhav_downloads_info - columns "date","success/failure"
# deliv_downloads_info - columns "date","success/failure"
# scrip_info columns - "name","date","open","high","low","close","vol", "deliv"
_DEF_SQLIITE_FNAME = '.nse_all_data.sqlite'

_bhav_dload_info_cr_stmt = '''CREATE TABLE IF NOT EXISTS bhav_downloads_info
                            (date TEXT, success boolean
                            check(success in (0,1)))'''
_deliv_dload_info_cr_stmt = '''CREATE TABLE IF NOT EXISTS deliv_downloads_info
                            (date TEXT, success boolean
                            check(success in (0,1)))'''
_scrip_info_cr_stmt = '''CREATE TABLE IF NOT EXISTS scrip_info
                    (name VARCHAR(64), date TEXT,
                        open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                        volume INTEGER, delivered INTEGER)'''


_create_stmts = {   'bhav_dload_info' : _bhav_dload_info_cr_stmt,
                    'deliv_dload_info' : _deliv_dload_info_cr_stmt,
                    'scrip_info' : _scrip_info_cr_stmt
                }

def get_bhavcopy(date='01-01-2002'):
    """Downloads a bhavcopy for a given date and returns a dictionary of rows where
    each row stands for a traded scrip. The scripname is key."""

    d2 = dt.strptime(date, '%d-%m-%Y')
    yr = d2.strftime('%Y')
    mon = d2.strftime('%b').upper()
    mm = d2.strftime('%0m')
    dd = d2.strftime('%0d')

    ddate = '%(yr)s-%(mm)s-%(dd)s' % {'yr':yr, 'mm':mm, 'dd':dd}

    if _bhavcopy_downloaded(ddate): ## already downloaded
        return None

    bhav_url = 'http://nseindia.com/content/historical/EQUITIES/' \
                '%(year)s/%(mon)s/cm%(dd)s%(mon)s%(year)sbhav.csv.zip' % \
                        ({'year':yr, 'mon':mon, 'dd':dd})

    deliv_url = 'http://nseindia.com/archives/equities/mto/' \
                'MTO_%(dd)s%(mm)s%(year)s.DAT' % \
                        ({'year':yr, 'mm':mm, 'dd':dd})

    x = requests.get(bhav_url)

    y = requests.get(deliv_url)
    stocks_dict = {}
    _update_dload_success(ddate, x.ok, y.ok)
    if x.ok and y.ok:
        z = ZipFile(bio(x.content))
        for name in z.namelist():
            csv_name = name
        delivery = bio(y.text)
        with z.open(csv_name) as bhav:
            i = 0
            for line in bhav:
                if i == 0:
                    i += 1
                    continue
                l = line.split(',')
                sym, o, h, l, c, v, d = l[0], l[2], l[3], l[4], \
                                                l[5], l[8], l[8]
                stocks_dict[sym] = [float(o), float(h), float(l), float(c),
                                    int(v), int(d)]
        i = 0
        for line in delivery:
            if i == 0:
                i += 1
                continue
            l = line.split(',')
            sym, d = l[1].strip(), l[3].strip()
            stocks_dict[sym][-1] = int(d)
            i += 1
        for sym in stocks_dict.keys():
            stocks_dict[sym] = scrip_ohlcvd_info(*stocks_dict[sym])
        return stocks_dict

def _update_dload_success(date, bhav_ok, deliv_ok, fname=None):
    fname = fname or _DEF_SQLIITE_FNAME
    con = sqlite3.connect(fname)
    cursor = con.cursor()
    insert_stmt = 'INSERT INTO %(table)s VALUES ("%(date)s", %(success)d);'
    insert_stmt_final = insert_stmt % {'table':'bhav_downloads_info',
                                    'date' : date, 'success':int(bhav_ok)}
    result = cursor.execute(insert_stmt_final)
    print insert_stmt_final
    insert_stmt_final = insert_stmt % {'table':'deliv_downloads_info',
                                    'date' : date, 'success':int(bhav_ok)}
    cursor.execute(insert_stmt_final)
    print insert_stmt_final
    con.commit()
    cursor.close()

def _update_bhavcopy(date, stocks_dict, fname=None):
    fname = fname or _DEF_SQLIITE_FNAME
    con = sqlite3.connect(fname)
    cur = con.cursor()
    insert_stmt = '''INSERT INTO scrip_info VALUES("%(sym)s", "%(date)s",
                    %(o)f, %(h)f, %(l)f, %(c)f, %(v)d, %(d)d);'''
    for key, val in stocks_dict.iteritems():
        print key, val
        insert_stmt_final = insert_stmt % { 'sym' : key, 'date':date,
                                            'o' : val.open, 'h': val.high,
                                            'l' : val.low, 'c': val.close,
                                            'v' : val.volume, 'd': val.deliv }
        print insert_stmt_final
        cur.execute(insert_stmt_final)

    con.commit()
    con.close()

def _bhavcopy_downloaded(ddate, fname=None):
    fname = fname or _DEF_SQLIITE_FNAME
    con = sqlite3.connect(fname)
    cur = con.cursor()
    select_stmt = '''select count(*) from bhav_downloads_info where
                    date = "%(date)s";''' % {'date':ddate}
    cur.execute(select_stmt)
    x = cur.fetchone()

    return x[0] > 0


def _create_tables(fname=None):
    fname = fname or _DEF_SQLIITE_FNAME
    cursor = sqlite3.connect(fname)
    for stmt in _create_stmts.values():
        cursor.execute(stmt)
    cursor.close()

if __name__ == '__main__':
    _create_tables()
    get_bhavcopy('20-09-2015')
    stocks_dict = get_bhavcopy()
    if stocks_dict:
        _update_bhavcopy('20010201', stocks_dict)
