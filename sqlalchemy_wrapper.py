"""
A wrapper script that hides all the details behind SQLAlchemy Core.
"""

import datetime

from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, Float, Date, Boolean, Enum, BigInteger
from sqlalchemy import MetaData
from sqlalchemy import create_engine

from sqlalchemy.sql import select as select_expr_

select_expr = select_expr_

from bse_utils import BSEGroup

_DB_STR = 'sqlite:///nse_hist_data.sqlite3'
_METADATA  = MetaData(bind=_DB_STR)

def create_or_get_all_scrips_table():
    """
    Creates All Scrips Info Table.

    Following information is saved about all scripts that are traded on NSE
    and BSE.

    security_isin : Unique for every security. Used as primary key.
    company_name : Name of the company.
    nse_traded : Flag indicating whether this security is traded on NSE.
    nse_symbol : Symbol on NSE
    nse_start_date : Start date for NSE.
    nse_suspended : Flag indicating whether suspended on NSE
    bse_traded : Flag indicating whether the script is traded on BSE.
    bse_start_date : Start date on BSE.
    bse_id : BSE ID for the script.
    bse_group: BSE Group for the script.
    """

    table_name = 'all_scrips_info'
    if table_name not in _METADATA.tables :
        all_scrips_tbl = Table('all_scrips_info', _METADATA,
                Column('security_isin', String(16), primary_key=True),
                Column('company_name', String(80)),
                Column('nse_traded', Boolean, default=False),
                Column('nse_start_date', Date,
                            default=datetime.date(year=2001, day=1, month=1)),
                Column('nse_symbol', String(20)),
                Column('nse_suspended', Boolean, default=False),
                Column('bse_traded', Boolean, default=False),
                Column('bse_start_date', Date,
                            default=datetime.date(year=2001, day=1, month=1)),
                Column('bse_id', String(6)),
                Column('bse_symbol', String(20)),
                Column('bse_group', Enum(BSEGroup)),
                )
        all_scrips_tbl.create(checkfirst=True)
    else:
        all_scrips_tbl = _METADATA.tables[table_name]

    return all_scrips_tbl

def create_or_get_nse_bhav_deliv_download_info():
    """
    Creates a table indicating whether NSE Bhavcopy/Deliver data is downloaded.

    date: date for which data is downloaded
    bhav_success: boolean indicating whether data is downloaded for bhavcopy
    deliv_success: boolean indicating whether data is downloaded for delivery
    error_type : Number of times error occurred
    """

    table_name = 'nse_bhav_deliv_download_info'
    if table_name not in _METADATA.tables :
        nse_bhav_deliv_dl_info = Table('nse_bhav_deliv_download_info', _METADATA,
                    Column('download_date', Date),
                    Column('bhav_success', Boolean, default=False),
                    Column('deliv_success', Boolean, default=False),
                    Column('error_type', String(16), default="DLOAD_ERR"),
                    )
        nse_bhav_deliv_dl_info.create(checkfirst=True)
    else:
        nse_bhav_deliv_dl_info = _METADATA.tables[table_name]

    return nse_bhav_deliv_dl_info

def create_or_get_nse_deliv_download_info():
    """
    Creates a table indicating whether NSE Delivery data is downloaded.

    date: date for which data is downloaded
    success: boolean indicating whether data is downloaded
    """
    table_name = 'nse_deliv_download_info'
    if table_name not in _METADATA.tables:
        nse_deliv_dl_info = Table('nse_deliv_download_info', _METADATA,
                    Column('download_date', Date),
                    Column('success', Boolean, default=False)
                    )
        nse_deliv_dl_info.create(checkfirst=True)
    else:
        nse_deliv_dl_info = _METADATA.tables[table_name]

    return nse_deliv_dl_info

def create_or_get_nse_equities_hist_data():
    """
    Creates a table of NSE Equities Historical Data.

    Since we download NSE historical data as a bhavcopy, we are storing this
    data in a DB because we are downloading using bhavcopy files and we need
    to have this in the stock:ohlcvd format.

    symbol : Symbol for the security traded.
    date : Date of the security traded.
    open : Open price
    high : High price
    low  : Low price
    close: Close price.
    volume: Total traded volume
    delivery : Total delivery for the security for the day.
    """
    table_name = 'nse_equities_hist_data'
    if table_name not in _METADATA.tables:
        nse_eq_hist_data = Table('nse_equities_hist_data', _METADATA,
                            Column('symbol', String(64)),
                            Column('date', Date),
                            Column('open', Float),
                            Column('high', Float),
                            Column('low', Float),
                            Column('close', Float),
                            Column('volume', BigInteger),
                            Column('delivery', BigInteger)
                            )
        nse_eq_hist_data.create(checkfirst=True)
    else:
        nse_eq_hist_data = _METADATA.tables[table_name]

    return nse_eq_hist_data

def create_or_get_nse_indices_hist_data():
    """
    Creates table for NSE Indices Historical data.

    Each row is of the form -

    symbol : Index symbol (our internal symbol)
    date : Date for the values
    open : open value
    high : high valu
    low : low value
    close : close value.

    We don't need other data like volume/delivery.
    """
    table_name = 'nse_indices_hist_data'
    if table_name not in _METADATA.tables:
        nse_idx_hist_data = Table('nse_indices_hist_data', _METADATA,
                            Column('symbol', String(64)),
                            Column('date', Date),
                            Column('open', Float),
                            Column('high', Float),
                            Column('low', Float),
                            Column('close', Float),
                            )
        nse_idx_hist_data.create(checkfirst=True)
    else:
        nse_idx_hist_data = _METADATA.tables[table_name]

    return nse_idx_hist_data

def create_or_get_nse_indices_info():
    """
    Creates NSE Indices info table.

    NSE keeps changing Index names, so we define our own names and only change
    index names as NSE likes it (this affects downloading certain data).

    symbol : Our symbol for the index
    name : NSE's current name for the index
    start_date : Date from which historical OHLC data is available.
    """
    table_name = 'nse_indices_info'
    if table_name not in _METADATA.tables:
        nse_idx_info = Table('nse_indices_info', _METADATA,
                        Column('symbol', String(64)),
                        Column('name', String(100)), # We hope ;-)
                        Column('start_date', Date)
                        )
        nse_idx_info.create(chechfirst=True)
    else:
        nse_idx_info = _METADATA.tables[table_name]

    return nse_idx_info

def get_engine():
    return _METADATA.bind

def execute_one(statement, results='all'):

    engine = _METADATA.bind

    result = engine.execute(statement)

    return result

def execute_many(statements, results = 'all'):

    engine = _METADATA.bind

    # FIXME : This is fugly for bulk inserts - Let's figure out what's the
    #         recommended way and then do it
    many_results = []
    for statement in statements:
        result = engine.execute(statement)
        many_results.append(result)

    return many_results

if __name__ == '__main__':
    print(all_scrips_table())

