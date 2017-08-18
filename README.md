
This is a work in progress. Most of the NSE stuff is usable, but not complete.

## Files Overview

Important modules are as follows -
* `all_stocks_list` - A utility that populates a DB table of all stocks traded on BSE and NSE. See the respective Util files for the stocks that are of interest to us. Utility - would likely 'stay in this repository'.
* `get_stocks_nse` - New way of downloading NSE stocks data. Application - will stay here.
* `get_stocks_bse` - Download data for individual stocks traded on BSE. Application - will stay here.
* `get_indices_nse` - Download data for indices traded on NSE. Application - will stay here.
* `corp_actions_nse` - Downloading corproate actions data for NSE. Mainly used to get Bonus/Split data to adjust historical prices. Application - will stay here.
* `scrip_to_h5` - Few experiments with HDF5 store (not usable yet). (Most likely Deprecated), will go away
* `read_sql_data` - (Will move to 'library' repo.)
* `process_pd_panel_lc` and `process_pd_panel_vector` - Experimental files, will stay here.
* `corp_actions_bse` - Not in usable form. Will mostly be deprecated.


### Downloading Data

~`get_stock_nse` and `get_stocks_nse2` are used to download OHLCVD historical
data for a stock. get\_stock\_nse2 - uses bhavcopy, ~get\_stocks\_nse~ is
observed to be a bit unreliable, hence the second approach is the supported one.~

`get_stock_nse` is used to download historical data sing bhavcopy method.

`get_stocks_bse` is used to download OHLCVD historical data for BSE scrips.

This downloaded data is kind of a staging data. Most of this data is maintained
inside an SQLite3 DB. (So someone can directly go and use it).

`corp_actions_nse` and `corp_actions_bse` are used to get corp actions for an
NSE or a BSE stock. NSE version is always preferred. BSE version is incomplete
yet as there are a number of issues with that (eg. ex\_date is missing in many
cases, stocks split ratio is missing.) To really fill this data we'd do it
manually later.

`get_indices_bse` and `get_indices_nse` are used to get indices historical data
from 1-Jan-2002 or whichever is earlier. These two scrips need a bit more work.

`all_stocks_list.py` - is a script that generates 'master list' of all stocks
traded on either NSE or BSE. We do not take stocks from all 'groups' in BSE
we only take group A, B and T (D and DT - we may but they are not very liquid
and hence not very useful). For NSE we are only interested in EQ and BE series

~The structure of HDF file is being iterated, so it's really very very early
right now to discuss in details.~

