#
# Refer to LICENSE file and README file for licensing information.
#
"""
Generates a master list of all stocks from BSE and NSE that we are interested
in. Stores it in the DB using ISIN as the key. This is uniq so we don't have
to worry about the BSE ID, NSE Symbol or BSE Symbol. The data that is stored is
in the following format

security_id: 'isin' (always unique)
company: 'Company Name'
nse_symbol : 'NSE Symbol'
nse_start_date : 'NSE Listing Date'
bse_symbol: 'BSE Symbol'
bse_start_date: 'BSE Listing Date'
bse_status : 'Active/Suspended'
bse_id : 'BSE ID'
bse_group : 'BSE Group'

Periodically we run this to update the table.
"""

_DEF_SQLITE_FNAME = '.all_stocks_data.sqlite3'
_all_scrips_info_cr_stmt = '''CREATE TABLE IF NOT EXISTS all_scrips_info
                            (security_isin VARCHAR(16) PRIMARY KEY,
                            company_name VARCHAR(80),
                            nse_present BOOLEAN check(nse_present in (0,1)),
                            nse_symbol VARCHAR(20),
                            nse_start_date FLOAT,
                            bse_present BOOLEAN check(nse_present in (0,1)),
                            bse_symbol VARCHAR(20),
                            bse_start_date FLOAT,
                            bse_id CHAR(6),
                            bse_group CHAR(2)); '''

_all_scrips_common_ins_stmt = '''INSERT INTO all_scrips_info values
                                ("%(isin)s", "%(name)s",
                                %(npresent)d, "%(nsymbol)s", %(ndate)f,
                                %(bpresent)d, "%(bsymbol)s", %(bdate)f,
                                "%(bid)s", "%(bgroup)s");'''

_all_scrips_bseonly_ins_stmt = '''INSERT INTO all_scrips_info
                                (security_isin, company_name,
                                nse_present, bse_present,
                                bse_symbol, bse_id, bse_group) values
                                ("%(isin)s", "%(name)s",
                                %(npresent)d, %(bpresent)d,
                                "%(bsymbol)s", "%(bid)s", "%(bgroup)s");'''

_all_scrips_nseonly_ins_stmt = '''INSERT INTO all_scrips_info
                                (security_isin, company_name,
                                nse_present, bse_present,
                                nse_symbol, nse_start_date) values
                                ("%(isin)s", "%(name)s",
                                %(npresent)d, %(bpresent)d,
                                "%(nsymbol)s", %(ndate)f);'''

from nse_utils import nse_get_all_stocks_list
from bse_utils import bse_get_all_stocks_list
from utils import get_ts_for_datestr

from sqlalchemy_wrapper import all_scrips_table

def get_nse_stocks_dict():
    nse_stocks_dict = {} # dictionary of nse stocks key = isin
    for nse_stock in nse_get_all_stocks_list():
        nse_stocks_dict[nse_stock.isin] = nse_stock
    return nse_stocks_dict

def get_bse_stocks_dict():
    bse_stocks_dict = {} # dictionary of bse stocks key = isin
    for bse_stock in bse_get_all_stocks_list():
        bse_stocks_dict[bse_stock.isin] = bse_stock
    return bse_stocks_dict

def populate_all_scrips_table():
    """
    Populates the all_scrips_info table.
    """
    nse_stocks_dict = get_nse_stocks_dict()
    nse_isins = nse_stocks_dict.keys()

    bse_stocks_dict = get_bse_stocks_dict()
    bse_isins = bse_stocks_dict.keys()

    common_isins = set(nse_isins) & set(bse_isins)
    only_bse_isins = set(bse_isins) - common_isins
    only_nse_isins = set(nse_isins) - common_isins

    for isin in common_isins:
        nstock = nse_stocks_dict[isin]
        bstock = bse_stocks_dict[isin]
        pass

    for isin in bse_only_isins:
        bstock = bse_stocks_dict[isin]
        pass

    for isin in nse_only_isins:
        nstock = nse_stocks_dict[isin]
        pass


import sqlite3

def create_all_stocks_tbl():
    global _all_scrips_info_cr_stmt

    with sqlite3.connect(_DEF_SQLITE_FNAME) as con:
        cur = con.cursor()
        cur.execute(_all_scrips_info_cr_stmt)
        con.commit()

if __name__ == '__main__':
    create_all_stocks_tbl()

    print common_isins & only_bse_isins
    print common_isins & only_nse_isins

    con = sqlite3.connect(_DEF_SQLITE_FNAME)
    cur = con.cursor()

    for isin in common_isins:
        nstock = nse_stocks_dict[isin]
        bstock = bse_stocks_dict[isin]
        ndate = get_ts_for_datestr(nstock.listing_date,'%d-%b-%Y')
        insert_stmt_final = _all_scrips_common_ins_stmt % \
                            { 'isin': nstock.isin, 'name': nstock.name,
                                'npresent' : 1, 'nsymbol': nstock.symbol,
                                'ndate': ndate,
                                'bpresent' : 1, 'bsymbol': bstock.symbol,
                                'bid': bstock.bseid, 'bgroup': bstock.group,
                                'bdate': ndate
                            }
        print insert_stmt_final
        cur.execute(insert_stmt_final)

    for isin in only_bse_isins:
        bstock = bse_stocks_dict[isin]
        insert_stmt_final = _all_scrips_bseonly_ins_stmt % \
                            { 'isin': bstock.isin, 'name': bstock.name,
                                'npresent' : 0, 'bpresent' : 1,
                                'bsymbol': bstock.symbol, 'bid': bstock.bseid,
                                'bgroup': bstock.group
                            }
        print insert_stmt_final
        cur.execute(insert_stmt_final)

    for isin in only_nse_isins:
        nstock = nse_stocks_dict[isin]
        ndate = get_ts_for_datestr(nstock.listing_date,'%d-%b-%Y')
        insert_stmt_final = _all_scrips_nseonly_ins_stmt % \
                            { 'isin': nstock.isin, 'name': nstock.name,
                                'npresent' : 1, 'bpresent' : 0,
                                'nsymbol': nstock.symbol, 'ndate': ndate
                            }
        print insert_stmt_final
        cur.execute(insert_stmt_final)

    con.commit()
    con.close()
