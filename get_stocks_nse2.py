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

import os
from utils import get_logger
module_logger = get_logger(os.path.basename(__file__))

# BIG FIXME: There are sql statements littered all over the place, sqlalchemy?

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

_BHAV_HEADERS =   {'Host': 'www.nseindia.com',
             'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
             'Accept':'application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
             'Accept-Encoding':'gzip, deflate, br',
             'Referer':'https://www.nseindia.com/product/content/equities/equities/archives_eq.htm',
             'Connection': 'keep-alive',
             'DNT':'1'}

import random
import time

from zipfile import ZipFile

from nse_utils import nse_get_name_change_tuples, ScripOHLCVD
import utils

from sqlalchemy_wrapper import create_or_get_nse_bhav_deliv_download_info, \
                                create_or_get_nse_equities_hist_data
from sqlalchemy_wrapper import execute_one, execute_one_insert, execute_many_insert
from sqlalchemy_wrapper import select_expr, and_expr

_date_fmt = '%d-%m-%Y'

_bhav_url_base = 'https://www.nseindia.com/content/historical/EQUITIES/' \
                '%(year)s/%(mon)s/cm%(dd)s%(mon)s%(year)sbhav.csv.zip'

_deliv_url_base = 'https://www.nseindia.com/archives/equities/mto/' \
                'MTO_%(dd)s%(mm)s%(year)s.DAT'

# Warn user if number of days data is greater than this
_WARN_DAYS = 100

def get_bhavcopy(date='01-01-2002'):
    """Downloads a bhavcopy for a given date and returns a dictionary of rows
    where each row stands for a traded scrip. The scripname is key. If the
    bhavcopy for a given day is already downloaded, returns None. date is in
    DD-MM-YYYY format"""

    global _date_fmt
    if isinstance(date, str):
        d2 = dt.date(dt.strptime(date, _date_fmt))
        strdate = date
    elif isinstance(date, dt):
        d2 = dt.date(date)
        strdate = dt.strftime(date, _date_fmt)
    else:
        return None
    yr = d2.strftime('%Y')
    mon = d2.strftime('%b').upper()
    mm = d2.strftime('%0m')
    dd = d2.strftime('%0d')

    if _bhavcopy_downloaded(d2): ## already downloaded
        return None

    global _bhav_url_base
    global _deliv_url_base

    bhav_url = _bhav_url_base % ({'year':yr, 'mon':mon, 'dd':dd})
    deliv_url = _deliv_url_base % ({'year':yr, 'mm':mm, 'dd':dd})

    try:
        x = requests.get(bhav_url, headers=_BHAV_HEADERS)
        module_logger.info("GET:Bhavcopy URL: %s" % bhav_url)

        y = requests.get(deliv_url, headers=_BHAV_HEADERS)
        module_logger.info("GET:Delivery URL: %s" % deliv_url)
    except requests.RequestException as e:
        module_logger.exception(e)
        # We don't update bhav_deliv_downloaded here
        return None

    stocks_dict = {}

    # We do all of the following to avoid - network calls
    error_code = None
    if x.status_code == 404 or y.status_code == 404:
        error_code = 'NOT_FOUND'
    else:
        if not (x.ok and y.ok):
            error_code = 'DLOAD_ERR'

    _update_dload_success(d2, x.ok, y.ok, error_code)

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
                if l[1] not in ['EQ', 'BE', 'BZ']:
                    continue
                sym, o, h, l, c, v, d = l[0], l[2], l[3], l[4], \
                                                l[5], l[8], l[8]
                stocks_dict[sym] = [float(o), float(h), float(l), float(c),
                                    long(v), long(d)]

        i = 0
        for line in delivery:
            if not line.startswith('20'):
                i += 1
                continue
            l = line.split(',')
            if (len(l)) == 4:
                sym, d = l[1].strip(), l[3].strip()
            elif len(l) == 7:
                if l[3] not in ['EQ', 'BE', 'BZ']:
                    i += 1
                    continue
                sym, d = l[2].strip(), l[5].strip()
            try:
                stocks_dict[sym][-1] = int(d)
            except KeyError:
                module_logger.error("For Symbol: %s Delivery Data found but no Bhavcopy Data" % sym)
            i += 1

        for sym in stocks_dict.keys():
            stocks_dict[sym] = ScripOHLCVD(*stocks_dict[sym])
            module_logger.debug("ScripInfo(%s): %s" % (sym, str(stocks_dict[sym])))

        return stocks_dict
    else:
        if not x.ok:
            module_logger.error("GET:Bhavcopy URL %s (%d)" % \
                                        (bhav_url, x.status_code))
        if not y.ok:
            module_logger.error("GET:Delivery URL %s (%d)" % \
                                        (deliv_url, y.status_code))
        return None

def _update_dload_success(fdate, bhav_ok, deliv_ok, error_code=None):
    """ Update whether bhavcopy download and delivery data download for given
    date is successful"""

    tbl = create_or_get_nse_bhav_deliv_download_info()

    sel_st = select_expr([tbl]).where(tbl.c.download_date == fdate)

    res = execute_one(sel_st)
    if not res.returns_rows:
        ins_or_upd_st = tbl.insert().values(download_date=fdate,
                                        bhav_success=bhav_ok,
                                        deliv_success=deliv_ok,
                                        error_type=error_code)
    else:
        ins_or_upd_st = tbl.update().where(tbl.c.download_date == fdate).\
                                        values(download_date=fdate,
                                            bhav_success=bhav_ok,
                                            deliv_success=deliv_ok,
                                            error_type=error_code)
    module_logger.debug(ins_or_upd_st.compile().params)

    #FIXME : check
    res.close()

    result = execute_one_insert(ins_or_upd_st)
    result.close()

def _update_bhavcopy(curdate, stocks_dict, fname=None):
    """update bhavcopy Database date in DD-MM-YYYY format."""

    nse_eq_hist_data = create_or_get_nse_equities_hist_data()

    # delete for today's date if there's anything FWIW
    d = nse_eq_hist_data.delete(nse_eq_hist_data.c.date == curdate)
    execute_one(d)

    insert_statements = []
    for k,v in stocks_dict.iteritems():
        ins = nse_eq_hist_data.insert().values(symbol=k, date=curdate,
                                                open=v.open, high=v.high,
                                                low=v.low, close=v.close,
                                                volume=v.volume,
                                                delivery=v.deliv)
        insert_statements.append(ins)
        module_logger.debug(ins.compile().params)

    results = execute_many_insert(insert_statements)
    for r in results:
        r.close()

def _bhavcopy_downloaded(fdate, fname=None):
    """
    Returns success/failure for a given date if bhav/delivery data.
    """
    tbl = create_or_get_nse_bhav_deliv_download_info()

    select_st = tbl.select().where(tbl.c.download_date == fdate)

    result = execute_one(select_st.compile())
    result = result.fetchone()

    if not result:
        return False

    # For anything older than 7 days from now, if Error is not found,
    # we ignore this.
    d = dt.date(dt.today())
    delta = d - fdate
    ignore_error = False
    if (delta.days > 7) and result.error_type == 'NOT_FOUND':
        ignore_error = True

    return (result[1] and result[2]) or ignore_error

def _apply_name_changes_to_db(syms, fname=None):
    """Changes security names in nse_hist_data table so the name of the security
    is always the latest."""

    hist_data = create_or_get_nse_equities_hist_data()

    update_statements = []
    for sym in syms:
        old = sym[0]
        new = sym[1]
        chdate = sym[2]

        chdt = dt.date(dt.strptime(chdate, '%d-%b-%Y'))

        upd = hist_data.update().values(symbol=new).\
                where(and_expr(hist_data.c.symbol == old,
                            hist_data.c.date < chdt))

        update_statements.append(upd)

    results = execute_many_insert(update_statements)
    for r in results:
        r.close()

def main(args):
    # We run the full program
    import argparse
    parser = argparse.ArgumentParser()

    # --full option
    parser.add_argument("--full-to",
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

    # --yes option
    parser.add_argument("--yes",
                        help="Answer yes to all questions.",
                        dest="sure",
                        action="store_true")

    args = parser.parse_args()
    try:
        from_date = dt.strptime(args.fromdate, _date_fmt)
        if args.todate.lower() == 'today':
            args.todate = dt.now().strftime(_date_fmt)
        to_date = dt.strptime(args.todate, _date_fmt)
    except ValueError:
        print parser.format_usage()
        return -1

    # We are now ready to download data
    if from_date > to_date:
        print parser.format_usage()
        return -1

    num_days = to_date - from_date

    if num_days.days > _WARN_DAYS:
        if args.sure:
            sure = True
        else:
            sure = raw_input("Tatal number of days for download is %1d. "
                             "Are you Sure?[y|N] " % num_days.days)
            if sure.lower() in ("y", "ye", "yes"):
                sure = True
            else:
                sure = False
    else:
        sure = True

    if not sure:
        sys.exit(0)

    module_logger.info("Downloading data for {0} days".format(num_days.days))

    cur_date = from_date
    while cur_date <= to_date:
        scrips_dict = get_bhavcopy(cur_date)
        if scrips_dict is not None:
            _update_bhavcopy(cur_date, scrips_dict)

        time.sleep(random.randrange(1,10))

        cur_date += td(1)

    # Apply the name changes to the DB
    sym_change_tuples = nse_get_name_change_tuples()
    if len(sym_change_tuples) == 0:
        module_logger.info("No name change tuples found...")
        sys.exit(-1)
    _apply_name_changes_to_db(sym_change_tuples)

    return 0


if __name__ == '__main__':

    import sys

    sys.exit(main(sys.argv[1:]))
