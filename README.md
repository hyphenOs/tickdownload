
This is a work in progress. A few notes about how all the utils fit together

nse\_utils.py and bse\_utils.py - provide NSE and BSE specific functionality  
   nse\_utils provides following API 
     - nse_get_all_stocks_list - Returns a generator of all stocks  
     - nse_get_name_change_tuples - Returns a tuple of name changes
     - ScripInfoOHLCVD - A named tuple containing the OHLCVD data for a day
     - ScripBaseinfoNSE - A named tuple for info about scrip on NSE
  
   bse\_utils provides following API
     - bse_get_all_stocks_list - Returns a generator of all stocks 
     - ScripBaseinfoBSE - A named tuple for info about a scrip on BSE

get\_stock\_nse and get\_stocks\_nse2 are used to download OHLCVD historical 
data for a stock. (get\_stock\_nse2 - uses bhavcopy, get\_stocks\_nse is 
observed to be a bit unreliable, hence the second approach is preferred.

get\_stocks\_bse is used to download OHLCVD historical data for BSE scrips.

This downloaded data is kind of a staging data. Most of this data is maintained
inside an SQLite3 DB. (So someone can directly go and use it). 

corp\_actions\_nse and corp\_actions\_bse are used to get corp actions for an
NSE or a BSE stock. NSE version is always preferred. BSE version is incomplete
yet as there are a number of issues with that (eg. ex\_date is missing in many
cases, stocks split ratio is missing.) To really fill this data we'd do it 
manually later.

get\_indices\_bse and get\_indices\_nse are used to get indices historical data
from 1-Jan-2002 or whichever is earlier. These two scrips need a bit more work.

all\_stocks\_list.py - is a script that generates 'master list' of all stocks
traded on either NSE or BSE. We do not take stocks from all 'groups' in BSE
we only take group A, B and T (D and DT - we may but they are not very liquid
and hence not very useful). 

utils.py - contains utilities that may not be NSE or BSE specific.

Most of this data is stored in HDF5 files for high performance using Pandas.

The structure of HDF file is being iterated, so it's really very very early 
right now to discuss in details. 

