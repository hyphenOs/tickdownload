#
# Refer to LICENSE file and README file for licensing information.
#
"""
Utility functions used by other modules - eg. getting a List of stocks

A little bit about BSE 'groups'

A Group - Highly liquid and high market cap
Z Group - Stocks not in dmat form - we are not interested in those
D Group - Stocks listed with relaxed listing norms, previously listed on RSEs
T Group - Stocks in Trade for Trade (under survelliance)
M Group - SME Perhaps?
B Group - Everything else
DT - D group Trade for trade
There are others

IP - No idea what they are

We'd be interested in A, B, D, T mainly

"""
from utils import get_logger
import os
module_logger = get_logger(os.path.basename(__file__))

import enum
class BSEGroup(enum.Enum):
    A = 'A'
    B = 'B'
    Z = 'Z'
    D = 'D'
    M = 'M'
    IP = 'IP'
    DT = 'DT'
    T = 'T'

import requests
import bs4
import sys

from collections import namedtuple

# The following two are global because we want to quickly update them if req
# but we don't want them to be imported. This is very internal
_GROUPS_INTERESTED = ('A', 'B', 'T', 'D')
_STOCKS_LIST_URL = 'http://www.bseindia.com/corporates/List_Scrips.aspx?'\
                    'expandable=1'

ScripBaseinfoBSE = namedtuple('ScripBaseinfoBSE',
                        ['bseid', 'symbol', 'name', 'group', 'isin'])

def bse_get_all_stocks_list(start=None, count=-1):
    """ Downloads the List of All Active stocks in BSE in groups A,B,T,D,DT
        If optional start and count are given, only downloads a subset from
        start -> start + count
    """

    start = start or 0

    try:
        start = int(start) or 0
        count = int(count) or -1
    except ValueError: # Make sure both start and count can be 'int'ed
        raise

    module_logger.info("GET: %s" % _STOCKS_LIST_URL)
    x = requests.get(_STOCKS_LIST_URL)

    if not x.ok:
        module_logger.error("Error(GET): %s", _STOCKS_LIST_URL)
        raise StopIteration # FIXME : raise correct exception

    html = bs4.BeautifulSoup(x.text, 'html.parser')

    hidden_elems = html.findAll(attrs={'type':'hidden'})

    form_data = {}
    for el in hidden_elems:
        m = el.attrs
        if m and m.has_key('value'):
            form_data[m['name']] = m['value']

    other_data = {
            'WINDOW_NAMER' : '1',
            'myDestination': '#',
            'ctl00$ContentPlaceHolder1$hdnCode' : '',
            'ctl00$ContentPlaceHolder1$ddSegment' : 'Equity',
            'ctl00$ContentPlaceHolder1$ddlStatus' : 'Active',
            'ctl00$ContentPlaceHolder1$getTExtData' : '',
            'ctl00$ContentPlaceHolder1$ddlGroup' : 'Select',
            'ctl00$ContentPlaceHolder1$ddlIndustry' : 'Select',
    }

    buttons_data = {
            'ctl00$ContentPlaceHolder1$btnSubmit.x' : '34',
            'ctl00$ContentPlaceHolder1$btnSubmit.y' : '8' }

    more_data_2 = { '__EVENTTARGET' : 'ctl00$ContentPlaceHolder1$lnkDownload',
                '__EVENTARGUMENT' : '' }

    form_data.update(other_data)
    form_data.update(buttons_data)

    module_logger.info("POST(1): %s" %  _STOCKS_LIST_URL)
    module_logger.debug("POST(1) Data: %s" % form_data)
    y = requests.post(_STOCKS_LIST_URL, data=form_data, stream=True)
    if not y.ok:
        module_logger.error("Error(POST(1)): %s" % _STOCKS_LIST_URL)
        raise StopIteration

    html2 = bs4.BeautifulSoup(y.text, 'html.parser')
    hidden_elems = html2.findAll(attrs={'type':'hidden'})

    form_data = {}
    for el in hidden_elems:
        m = el.attrs
        if m and m.has_key('value'):
            form_data[m['name']] = m['value']

    form_data.update(other_data)
    form_data.update(more_data_2)

    module_logger.info("POST(2): %s" % _STOCKS_LIST_URL)
    module_logger.debug("POST(2) Data: %s" % form_data)
    y = requests.post(_STOCKS_LIST_URL, data=form_data, stream=True)
    if not y.ok:
        module_logger.error("Error(POST(2)): %s", _STOCKS_LIST_URL)
        raise StopIteration # FIXME: Raise a correct error

    i = 0
    for line in y.text.split("\n"):
        line = line.split(",")
        if len(line) < 9:
            module_logger.debug("Unparsed: %s" % line)
            continue
        if line[0].lower().strip() == 'security code':
            continue
        group = line[4].strip()
        if group not in _GROUPS_INTERESTED:
            continue
        if i < start:
            i += 1
            continue

        if count > 0 and i >= start+count:
            raise StopIteration

        bse_id = line[0].strip()
        symbol = line[1].strip().upper()
        name = line[2].strip()
        group = line[4].strip()
        isin = line[6].strip()
        i += 1
        bse_scrip_info = ScripBaseinfoBSE(bse_id, symbol, name, group, isin)
        module_logger.debug("ScripInfo: %s" % str(bse_scrip_info))
        yield bse_scrip_info


if __name__ == '__main__':
    for x in bse_get_all_stocks_list(count=-1):
        print x
