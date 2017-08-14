What information about a stock "could be useful"

- Exchange where a stock is traded
- Comparison of stock with broad based market index (CNX 500 and NIFTY say)
- If stock is part of any indices - it's performance with respect to those indices
- Ownership of stock by mutual fund (good to have would be an indication of up or down)
- Liquidity (3 month average volume as a percentile)
- Chart (of course) going back max up to 10 years.
- Dividends/splits etc (on chart)
- Returns based "score" - (percentile score) compared to all returns. for 1m/1w/3m/1y/ytd


Workflow
--------

1. Companies information -
   - ~First time download and populate~

   - Every weekend download and compare
     - Get all ISINs from URL
     - Get all unique ISINs from Database
     - find urls only ISINs and DB only ISINs
     - If there are new ISINs, chances are those are new listings and then look at NSE
       symbols etc. to figure out they re indeed new listings.
     - Make sure to match symbol as well as start date, just to make sure, if match
       found, update the row or else add a new row.

2. Company name change information
   - Not required for the first data population

   - Every weekend 'after' the `all_scrips_info` table is updated, update this information.
   - Search for 'old company name'  and then update with 'new company name'.

3. See Scrips section below.

Scrips
------
1. `first_time.sh` - The script used to populate data for the first time. This script performs following actions -
   * Downloads all equities data as bhavcopy.
   * Downloads all corp-actions bonus/split data for all stocks and stores in DB.
   * Downloads all indices data from the beginning for all 'supported indices'.

2. `daily.sh` - This script is to be run typically daily, a good idea may be to run it throgh a `cron` job. This script performs following actions -
   * Downloads stocks data for past 2 weeks for all stocks (if data already exists in the DB, it is not downloaded).
   * Downloads indices data for past 2 weeks for all 'supported indices'.
   * Downloads corp-actions bonus/split data. (TODO)
   * Downloads company name changes data and applies to current data. Updates all symbol changes in the `all_scrips_info` tabe. (TODO)

3. `weekly.sh`
   * This script is run every week. This script mainly updates the - master data if the ISIN is changed. (TODO)

