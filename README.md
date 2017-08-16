
This is a work in progress. Most of the NSE stuff is usable, but not complete.

## Big changes coming to this repository.

Currently this repository is like a mix of library functions (that will be useful, hopefully) in a few locations and mostly - wrapper scripts/utils that really belong to this repository. Correspondingly, following things are likely to change (see below Files overview).

## Files Overview

Important files are as follows -
* `all_stocks_list` - A utility that populates a DB table of all stocks traded on BSE and NSE. See the respective Util files for the stocks that are of interest to us. Utility - would likely 'stay in this repository'.
* `nse_utils` - A utility for NSE specific stocks (see below for details). (Will move to 'library' repo)
* `bse_utils` - A utility for BSE specific stocks (see below for details). (Will move to 'library' repo)
* `utils` - Utiliies like logger that can be used by everyone. (Will move to 'library' repo)
* `sqlalchemy_wrapper` - Defines all major tables in database and utility functions that wrap [sqlalchemy](http://www.sqlalchemy.org/). (Will move to library repo).
* `get_stocks_nse2` - New way of downloading NSE stocks data. Application - will stay here.
* `get_stocks_bse` - Download data for individual stocks traded on BSE. Application - will stay here.
* `get_indices_nse` - Download data for indices traded on NSE. Application - will stay here.
* `corp_actions_nse` - Downloading corproate actions data for NSE. Mainly used to get Bonus/Split data to adjust historical prices. Application - will stay here.
* `scrip_to_h5` - Few experiments with HDF5 store (not usable yet). (Most likely Deprecated), will go away
* `db_read_into_pd` - Few experiments with Pandas Panel (not usable - more like a scratchpad). This code will eventually move out.
* `read_sql_data` - (Will move to 'library' repo.)
* `process_pd_panel_lc` and `process_pd_panel_vector` - Experimental files, will stay here.
* `corp_actions_bse` - Not in usable form. Will mostly be deprecated.
* `get_stocks_nse` - An old way of downloading NSE stocks data - deprecated, use `get_stocks_nse2` instead.


## API

The term API is being used very 'loosely' here. Mostly to document what users should expect.

### `nse_utils`

NSE Utils provides following Main APIs

- `nse_get_all_stocks_list` - Returns a generator of all stocks
- `nse_get_name_change_tuples` - Returns a tuple of name changes of the form `(old, new, date)`
- ScripInfoOHLCVD - A named tuple containing the OHLCVD data for a day
- ScripBaseinfoNSE - A named tuple for info about scrip on NSE

### `bse_utils`

- `bse_get_all_stocks_list` - Returns a generator of all stocks
- `ScripBaseinfoBSE` - A named tuple for info about a scrip on BSE

### Downloading Data

`get_stock_nse` and `get_stocks_nse2` are used to download OHLCVD historical
data for a stock. get\_stock\_nse2 - uses bhavcopy, ~get\_stocks\_nse~ is
observed to be a bit unreliable, hence the second approach is the supported one.

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

The structure of HDF file is being iterated, so it's really very very early
right now to discuss in details.

