#
# Refer to LICENSE file and README file for licensing information.
#
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

import random
import time

from zipfile import ZipFile

from nse_utils import get_name_change_tuples, scrip_ohlcvd_info
import utils

import sqlite3

# SQLite database for stocks data. There are following tables
# bhav_downloads_info - columns "date","success/failure"
# deliv_downloads_info - columns "date","success/failure"
# scrip_info columns - "name","date","open","high","low","close","vol", "deliv"
_DEF_SQLIITE_FNAME = '.nse_all_data.sqlite'

_bhav_dload_info_cr_stmt = '''CREATE TABLE IF NOT EXISTS bhav_downloads_info
                            (date FLOAT, success boolean
                            check(success in (0,1)))'''
_deliv_dload_info_cr_stmt = '''CREATE TABLE IF NOT EXISTS deliv_downloads_info
                            (date FLOAT, success boolean
                            check(success in (0,1)))'''
_scrip_info_cr_stmt = '''CREATE TABLE IF NOT EXISTS scrip_info
                    (name VARCHAR(64), date TEXT,
                        open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                        volume INTEGER, delivered INTEGER)'''


_create_stmts = {   'bhav_dload_info' : _bhav_dload_info_cr_stmt,
                    'deliv_dload_info' : _deliv_dload_info_cr_stmt,
                    'scrip_info' : _scrip_info_cr_stmt
                }

_date_fmt = '%d-%m-%Y'

def get_bhavcopy(date='01-01-2002'):
    """Downloads a bhavcopy for a given date and returns a dictionary of rows
    where each row stands for a traded scrip. The scripname is key. If the
    bhavcopy for a given day is already downloaded, returns None. date is in
    DD-MM-YYYY format"""

    global _date_fmt
    if isinstance(date, str):
        d2 = dt.strptime(date, _date_fmt)
        strdate = date
    elif isinstance(date, dt):
        d2 = date
        strdate = dt.strftime(date, _date_fmt)
    else:
        return None
    yr = d2.strftime('%Y')
    mon = d2.strftime('%b').upper()
    mm = d2.strftime('%0m')
    dd = d2.strftime('%0d')

    print yr, mm, dd
    fdate = utils.get_ts_for_datestr(strdate, _date_fmt)
    if _bhavcopy_downloaded(fdate): ## already downloaded
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
    _update_dload_success(fdate, x.ok, y.ok)
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
            if not line.startswith('20'):
                i += 1
                continue
            l = line.split(',')
            if (len(l)) == 4:
                sym, d = l[1].strip(), l[3].strip()
            elif len(l) == 7:
                sym, d = l[2].strip(), l[5].strip()
            stocks_dict[sym][-1] = int(d)
            i += 1
        for sym in stocks_dict.keys():
            stocks_dict[sym] = scrip_ohlcvd_info(*stocks_dict[sym])
        return stocks_dict

def _update_dload_success(fdate, bhav_ok, deliv_ok, fname=None):
    """ Update whether bhavcopy download and delivery data download for given
    date is successful"""
    fname = fname or _DEF_SQLIITE_FNAME
    with sqlite3.connect(fname) as con:
        cursor = con.cursor()
        insert_stmt = 'INSERT INTO %(table)s VALUES (%(date)f, %(success)d);'
        insert_stmt_final = insert_stmt % {'table':'bhav_downloads_info',
                                    'date' : fdate, 'success':int(bhav_ok)}
        result = cursor.execute(insert_stmt_final)
        print insert_stmt_final
        insert_stmt_final = insert_stmt % {'table':'deliv_downloads_info',
                                    'date' : fdate, 'success':int(bhav_ok)}
        cursor.execute(insert_stmt_final)
        print insert_stmt_final
        con.commit()

def _update_bhavcopy(strdate, stocks_dict, fname=None):
    """update bhavcopy Database date in DD-MM-YYYY format."""
    fname = fname or _DEF_SQLIITE_FNAME
    with sqlite3.connect(fname) as con:
        cur = con.cursor()

        fdate = utils.get_ts_for_datestr(strdate, _date_fmt)
        # just to be safe
        delete_stmt = '''DELETE FROM scrip_info where date = %(date)f'''
        delete_stmt_final = delete_stmt % { 'date': fdate}
        cur.execute(delete_stmt_final)
        insert_stmt = '''INSERT INTO scrip_info VALUES("%(sym)s", %(date)f,
                        %(o)f, %(h)f, %(l)f, %(c)f, %(v)d, %(d)d);'''
        for key, val in stocks_dict.iteritems():
            insert_stmt_final = insert_stmt % { 'sym' : key, 'date': fdate,
                                                'o' : val.open, 'h': val.high,
                                                'l' : val.low, 'c': val.close,
                                                'v' : val.volume, 'd': val.deliv
                                                }
            print "Executing:", insert_stmt_final
            cur.execute(insert_stmt_final)

        con.commit()

def _bhavcopy_downloaded(fdate, fname=None):
    fname = fname or _DEF_SQLIITE_FNAME
    with sqlite3.connect(fname) as con:
        cur = con.cursor()
        select_stmt = '''select count(*) from bhav_downloads_info where
                     date = %(date)f;''' % {'date':fdate}
        cur.execute(select_stmt)
    x = cur.fetchone()

    return x[0] > 0

def _create_tables(fname=None):
    fname = fname or _DEF_SQLIITE_FNAME
    with sqlite3.connect(fname) as cursor:
        for stmt in _create_stmts.values():
            cursor.execute(stmt)

def _apply_name_changes_to_db(syms, fname=None):
    """Changes security names in scrip_info table so the name of the security
    is always the latest."""
    fname = fname or _DEF_SQLIITE_FNAME
    update_stmt = '''update scrip_info set name = '{}' where name in ({})'''
    with sqlite3.connect(fname) as con:
        cur = con.cursor()
        for sym in syms:
            old = sym[:-1]
            new = sym[-1]
            olds = ','.join(["'%s'" % x for x in old])
            update_stmt_final = update_stmt.format(new, olds)
            cur.execute(update_stmt_final)
        con.commit()

if __name__ == '__main__':
    # We run the full program
    import argparse
    parser = argparse.ArgumentParser()

    # --full option
    parser.add_argument("--full",
                        help="download full data from 1 Jan 2002",
                        action="store_true")

    # --from option
    parser.add_argument("--from",
                        help="From Date in DD-MM-YYYY format. " \
                                "Default is 01-01-2002",
                        dest='fromdate',
                        default="01-01-2002")
    # --to option
    parser.add_argument("--to",
                        help="From Date in DD-MM-YYYY format. " \
                                "Default is Today.",
                        dest='todate',
                        default="today")
    args = parser.parse_args()
    try:
        _ = dt.strptime(args.fromdate, _date_fmt)
        if args.todate.lower() == 'today':
            args.todate = dt.now().strftime(_date_fmt)
        _ = dt.strptime(args.todate, _date_fmt)
    except ValueError:
        print parser.format_usage()
        sys.exit(-1)

    # We are now ready to download data
    from_date = dt.strptime(args.fromdate, _date_fmt)
    to_date = dt.strptime(args.todate, _date_fmt)
    if from_date > to_date:
        print parser.format_usage()
        sys.exit(-1)

    _create_tables()

    tdelta = to_date - from_date
    if tdelta.days > 50:
        "Downloading data for {0} days".format(tdelta.days)

    cur_date = from_date
    while cur_date <= to_date:
        scrips_dict = get_bhavcopy(cur_date)
        str_curdate = cur_date.strftime(_date_fmt)
        if scrips_dict is not None:
            _update_bhavcopy(str_curdate, scrips_dict)

        time.sleep(random.randrange(1,10))

        cur_date += td(1)

    # Apply the name changes to the DB
    sym_change_tuples = get_name_change_tuples()
    if len(sym_change_tuples) == 0:
        print "No name change tuples found..."
        sys.exit(-1)
    _apply_name_changes_to_db(sym_change_tuples)
