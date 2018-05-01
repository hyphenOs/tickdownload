#
# Refer to LICENSE file and README file for licensing information.
#
#pylint: disable-msg=broad-except

"""Get's data for corporate Actions for a given stock on NSE. Parses and
stores the bonus and face value splits and dividends.

For NSE we have data reliably from 1st Jan 2005. For older data, we might
have to refer to BSE data.

For every corp action, we output data in the following form -
scrip, ex_date, type(B:bonus,D:dividend,S:split), ratio, value.
ratio < 1.0 for bonus/split, value in Rs. for div"""

import os
import re
import time
import random

from collections import namedtuple
from datetime import datetime as dt

import requests

from tickerplot.nse.nse_utils import nse_get_all_stocks_list
from tickerplot.utils.logger import get_logger

from tickerplot.sql.sqlalchemy_wrapper import create_or_get_nse_corp_actions_hist_data
from tickerplot.sql.sqlalchemy_wrapper import execute_many_insert
from tickerplot.sql.sqlalchemy_wrapper import get_metadata

module_logger = get_logger(os.path.basename(__file__))

_DB_METADATA = None

#FIXME : Implement bonus/split processing from a specific date.

# _CorpActionAll is a named tuple of following type
#"Symbol","Company","Industry","Series","Face Value(Rs.)","Purpose",
# "Ex-Date","Record Date","BC Start Date","BC End Date",
# "No Delivery Start Date","No Delivery End Date"
_CorpActionAll = namedtuple('_CorpActionAll', ['sym', 'name', 'industry',
                                        'series', 'fv', 'purpose', 'ex_date',
                                        'rec_date', 'bc_sdate', 'bc_edate',
                                        'nd_sdate', 'nd_edate'])

CorpAction = namedtuple('CorpAction', ['sym', 'ex_date', 'action',
                                        'ratio', 'delta'])


# Don't ask me how I got these regex, lots of trial/error
_div_regex = re.compile(r'(?:.*?)(?P<div>(?:(?:div.*?)(\d+%)|(?:div.*?(rs\.?)?)\s*(\d+\.?\d*)))')
_rsr_regex = re.compile(r'rs\.?')
_num_per_r = re.compile(r'(\d+\.?\d{0,2}%?)')
_bonus_regex = re.compile(r'bon.*?(\d+)\s*:\s*(\d)')
_split_regex = re.compile(r'.*f(?:.*?)v.*.*?spl.*?(\d+).*?(\d+).*')

def _do_process_purpose(action):
    """ Does all the 'hard work' in processing the purpose. Returns a single
    line of the form
    symbol, ex_date(yyyy-mm-dd), purpose(d/b/s), ratio(for b/s), value(for d),
    """
    symbol = action.sym.upper()
    purpose = action.purpose.lower()
    ex_date = action.ex_date
    fv = float(action.fv)
    actions = []
    if purpose.find('div') >= 0:
        #r = re.compile(r'(?:.*?)(?P<Div>(?:div.*?)?((?:(rs.*?)|(\s+))\d+\.?\d*((?:\/-)|%)?))')
        #r = re.compile(r'(?:.*?)(?P<Div>(?:div.*?)((?:(rs\.*?)|(\s+))\d+\.?\d*(?:\/-)?)|(\d+%))')

        #r = re.compile(r'(?:.*?)(?P<div>(?:(?:div.*?)(\\d+%)|(?:div.*?(rs\\.?)?)\\s*(\\d+\\.?\\d*)))')

        for x in _div_regex.finditer(purpose):
            for _, v in x.groupdict().iteritems():
                v = re.sub(_rsr_regex, '', v)
                for y in _num_per_r.finditer(v):
                    z = y.group()
                    if z.find('%') > 0:
                        div = float(z.replace('%', '')) * (fv/100)
                    else:
                        div = float(z)
                actions.append(CorpAction(symbol, ex_date, 'D', 1.0, div))
    if purpose.find('bon') >= 0:
        y = _bonus_regex.search(purpose)
        if y:
            n, d = float(y.group(1)), float(y.group(2))
            ratio = n / (n+d)
            action = CorpAction(symbol, ex_date, 'B', ratio, 0.0)
            actions.append(action)
            module_logger.debug("CorpAction: %s", str(CorpAction))
    if purpose.find('spl') >= 0:
        y = _split_regex.search(purpose)
        if y:
            d, n = float(y.group(1)), float(y.group(2))
            ratio = n / d
            action = CorpAction(symbol, ex_date, 'S', ratio, 0.0)
            actions.append(action)
            module_logger.debug("CorpAction: %s", str(CorpAction))
    return actions


def _process_purpose(corp_actions):
    """ Takes a list of `corp_actions` named tuples and does lot of processing
    on text of the purpose and generates a list of comma separated strings of
    the following form.
    symbol, ex_date(yyyy-mm-dd), purpose(d/b/s), ratio(for b/s), value(for d),
    """
    actions = []
    for a in corp_actions:
        actions.extend(_do_process_purpose(a))
    return actions


def _process_ca_text(ca_text):
    """Processes a CA text and returns a list of strings of the format
    symbol,ex_date,purpose,value,ratio
    symbol: symbol - always present
    ex_date: may be absent - we've seen it in some cases
    purpose : could be 'D'vididend, 'B'onus, 'S'plit, 'M'erger
    """
    corp_actions = []
    for line in ca_text.split('\n'):
        l = [x.strip().replace('"','') for x in line.split(",")]
        if l[0].lower() == 'symbol':
            continue
        if len(l) < len(_CorpActionAll._fields):
            module_logger.info("Not Processed: %s", l)
            continue
        l[6] = dt.date(dt.strptime(l[6], '%d-%b-%Y'))
        a = _CorpActionAll(*l)
        corp_actions.append(a)
    return _process_purpose(sorted(list(set(corp_actions)),
                key=lambda x: x.ex_date))

def get_corp_action_csv(sym_name=None, time_period=None):
    """
    Get's the corp action CSV for the symbol name, if sym_name is specified or
    else get's corp action CSV for the time_period specifid. If both are
    specified, get's for both.
    """
    ca_text = ''
    if sym_name:
        sym_name = sym_name.upper().replace('&', '%26')
        base = 'http://nseindia.com/corporates/datafiles/'
        sym_part = 'CA_%s_LAST_24_MONTHS.csv' % sym_name
        url = base + sym_part
        module_logger.info("GET: %s", url)
        r = requests.get(url)
        if r.ok:
            ca_text += r.text
        else:
            module_logger.error("GET: %s(%d)", url, r.status_code)

        sym_part2 = 'CA_%s_MORE_THAN_24_MONTHS.csv' % sym_name
        url = base + sym_part2
        module_logger.info("GET: %s", url)
        r = requests.get(url)
        if r.ok:
            ca_text += r.text
        else:
            module_logger.error("GET: %s(%d)", url, r.status_code)

    if time_period:
        base = 'http://nseindia.com/corporates/datafiles/'
        sym_part = 'CA_LAST_%s.csv' % time_period
        url = base + sym_part
        module_logger.info("GET: %s", url)
        r = requests.get(url)
        if r.ok:
            ca_text += r.text
        else:
            module_logger.error("GET: %s(%d)", url, r.status_code)

    return _process_ca_text(ca_text)

def main(args):

    import argparse
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all",
                        help="Download data for all stocks. Usually you'd have "
                            "to do it only once.",
                        dest="all_stocks",
                        action="store_true")

    group.add_argument("--from",
                        help="Download data from this data. Date Format "
                            "'DD-MM-YYYY'.",
                        dest="from_date")


    # --dbpath option
    parser.add_argument("--dbpath",
                        help="Database URL to be used.",
                        dest="dbpath")

    args, unprocessed = parser.parse_known_args()

    # Make sure we can access the DB path if specified or else exit right here.
    if args.dbpath:
        try:
            global _DB_METADATA
            _DB_METADATA = get_metadata(args.dbpath)
        except Exception as e:
            print ("Not a valid DB URL: {} (Exception: {})".format(
                                                            args.dbpath, e))
            return -1

    all_corp_actions = []
    if args.all_stocks:
        unprocessed = (x.symbol for x in nse_get_all_stocks_list())

    # It's possible to give --from DD-MM-YYYY infy (say), just help
    for stock in unprocessed:
        time.sleep(random.randint(1,5))
        try:
            corp_actions = get_corp_action_csv(sym_name=stock)
        except Exception as e:
            module_logger.exception(e)
            continue
        all_corp_actions.extend(corp_actions)

    if args.from_date:
        try:
            from_date = dt.date(dt.strptime(args.from_date, '%d-%m-%Y'))
            today = dt.date(dt.now())
            td = today - from_date
            if td.days < 0:
                print("From date cannot be greater than today.")
                return -1
            if td.days < 15:
                corp_actions = get_corp_action_csv(time_period='15_DAYS')
            else:
                corp_actions = get_corp_action_csv(time_period='3_MONTHS')

            all_corp_actions.extend(corp_actions)

        except ValueError as e:
            print("Date '{}' in unsupported format".format(args.from_date))
            return -1

    tbl = create_or_get_nse_corp_actions_hist_data(metadata=_DB_METADATA)

    all_insert_statements = []
    for corp_action in all_corp_actions:
        module_logger.debug("CorpAction :%s", str(corp_action))
        insert_st = tbl.insert().values(symbol=corp_action.sym,
                                        ex_date=corp_action.ex_date,
                                        action=corp_action.action,
                                        ratio=corp_action.ratio,
                                        delta=corp_action.delta)
        all_insert_statements.append(insert_st)
        module_logger.debug("insert_st : %s", insert_st.compile().params)

    results = execute_many_insert(all_insert_statements,
                                    engine=_DB_METADATA.bind)
    for result in results:
        result.close()

    return 0

if __name__ == '__main__':

    import sys
    sys.exit(main(sys.argv[1:]))
