"""
Generates a master list of all stocks from BSE and NSE that we are interested
in. Stores it in the DB using inei as the key. This is uniq so we don't have
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

nse_stocks_dict = {} # dictionary of nse stocks key = isin
for nse_stock in nse_get_all_stocks_list():
    nse_stocks_dict[nse_stock.isin] = nse_stock

bse_stocks_dict = {} # dictionary of bse stocks key = isin
for bse_stock in bse_get_all_stocks_list():
    bse_stocks_dict[bse_stock.isin] = bse_stock

nse_isins = nse_stocks_dict.keys()
bse_isins = bse_stocks_dict.keys()

common_isins = set(nse_isins) & set(bse_isins)
only_nse_isins = set(nse_isins) - common_isins
only_bse_isins = set(bse_isins) - common_isins

for i in common_isins:
    print i

print "--------------------------------"

for i in only_nse_isins:
    print i

print "--------------------------------"


for i in only_bse_isins:
    print i


