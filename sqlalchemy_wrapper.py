"""
A wrapper script that hides all the details behind SQLAlchemy Core.
"""

import datetime

from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, Float, Date, Boolean, Enum
from sqlalchemy import MetaData

from bse_utils import BSEGroup

_METADATA  = MetaData()

def all_scrips_table():
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
            Column('bse_group', Enum(BSEGroup))
            )
    return all_scrips_tbl

if __name__ == '__main__':
    print(all_scrips_table())

