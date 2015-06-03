"""Get's data for corporate Actions for a given stock on NSE. Parses and
stores the bonus and face value splits.

For NSE we have data reliably from 1st Jan 2005. For older data, we might
have to refer to BSE data"""

import requests

from nse_utils import get_all_stocks_list
from collections import namedtuple

# CorpAction is a named tuple of following type
#"Symbol","Company","Industry","Series","Face Value(Rs.)","Purpose",
# "Ex-Date","Record Date","BC Start Date","BC End Date",
# "No Delivery Start Date","No Delivery End Date"
CorpAction = namedtuple('CorpAction', ['sym', 'name', 'industry',
                                        'series', 'fv', 'purpose','ex_date',
                                        'rec_date', 'bc_sdate', 'bc_edate',
                                        'nd_sdate', 'nd_edate'])
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
        if l[0].lower == 'symbol':
            continue
        if len(l) < len(CorpAction._fields):
            print ",".join(l), ' has fewer fields than required ', \
                    len(CorpAction._fields)
            continue
        corp_actions.append(CorpAction(*l))
    return corp_actions

def get_corp_action_csv(sym_name):
    """Get's the corp action CSV for the symbol name."""
    sym_name = sym_name.upper()
    base = 'http://nseindia.com/corporates/datafiles/'
    sym_part = 'CA_%s_LAST_24_MONTHS.csv' % sym_name
    url =  base + sym_part
    print "Getting...", url
    r = requests.get(url)
    if r.ok:
        ca_text = r.text
    else:
        return ''

    sym_part2 = 'CA_%s_MORE_THAN_24_MONTHS.csv' % sym_name
    url =  base + sym_part2
    print "Getting...", url
    r = requests.get(url)
    if r.ok:
        ca_text += r.text

    return _process_ca_text(ca_text)


if __name__ == '__main__':
    #for x in get_all_stocks_list(start=500, count=2):
    #    print get_corp_action_csv(x.symbol)
    print get_corp_action_csv('inFy')
