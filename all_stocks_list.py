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
from nse_utils import nse_get_all_stocks_list
from bse_utils import bse_get_all_stocks_list
from utils import get_datetime_for_datestr

import os
from utils import get_logger
module_logger = get_logger(os.path.basename(__file__))

from sqlalchemy_wrapper import create_all_scrips_table
from sqlalchemy_wrapper import execute_many

from datetime import datetime as dt

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
    bse_only_isins = set(bse_isins) - common_isins
    nse_only_isins = set(nse_isins) - common_isins

    t = create_all_scrips_table()

    count = 0
    insert_statements = []
    for isin in common_isins:
        nstock = nse_stocks_dict[isin]
        bstock = bse_stocks_dict[isin]

        nstart_datetime = get_datetime_for_datestr(
                                                datestr=nstock.listing_date,
                                                fmt='%d-%b-%Y')
        nstart_date = dt.date(nstart_datetime)

        ins = t.insert().values(security_isin=nstock.isin,
                                company_name=nstock.name,
                                nse_traded=True,
                                nse_start_date=nstart_date,
                                nse_symbol=nstock.symbol,
                                #nse_suspended default is False,
                                bse_traded=True,
                                bse_start_date=nstart_date,
                                bse_id=bstock.bseid,
                                bse_symbol=bstock.symbol,
                                bse_group=bstock.group)
        module_logger.debug(ins.compile().params)

        insert_statements.append(ins)
        count += 1

    common_count = count
    module_logger.info("common securities count: %d", common_count)

    for isin in bse_only_isins:
        bstock = bse_stocks_dict[isin]

        ins = t.insert().values(security_isin=bstock.isin,
                                company_name=bstock.name,
                                bse_traded=True,
                                bse_id=bstock.bseid,
                                bse_symbol=bstock.symbol,
                                bse_group=bstock.group)
        module_logger.debug(ins.compile().params)

        insert_statements.append(ins)
        count += 1

    bse_only_count = count - common_count
    module_logger.info("bse_only securities count: %d", bse_only_count)

    for isin in nse_only_isins:
        nstock = nse_stocks_dict[isin]

        nstart_datetime = get_datetime_for_datestr(nstock.listing_date,
                                                '%d-%b-%Y')
        nstart_date = dt.date(nstart_datetime)

        ins = t.insert().values(security_isin=nstock.isin,
                                company_name=nstock.name,
                                nse_traded=True,
                                nse_start_date=nstart_date,
                                nse_symbol=nstock.symbol
                                #nse_suspended default is False,
                                )
        module_logger.debug(ins.compile().params)

        insert_statements.append(ins)
        count += 1

    nse_only_count = count - bse_only_count - common_count
    module_logger.info("nse_only securities count: %d", nse_only_count)

    return insert_statements

if __name__ == '__main__':

    import sys

    insert_statements = populate_all_scrips_table()
    results = execute_many(insert_statements)

    sys.exit(0)
