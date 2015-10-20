import sqlite3

import pandas as pd

con = sqlite3.connect('.nse_all_data.sqlite')

scrips = pd.io.sql.read_sql('select distinct(name) from scrip_info;', con);

# First get a list of all scrips
lscrips = map(lambda x:x, scrips.name)

# now we've to create a panel and add each 'script' as an axis to panel.

print pd.version.version

scripinfo_dict = {}
i = 1
for scrip in lscrips:
    print scrip, i
    scripinfo_stmt = '''select open, high, low, close, volume, delivered,
                Date(date, "unixepoch") from scrip_info where name = "%s"'''
    scripinfo_stmt_final = scripinfo_stmt % scrip
    scripdata = pd.io.sql.read_sql(scripinfo_stmt_final, con)
    scripdata.columns = ['open', 'high', 'low', 'close', 'volume',
                            'delivered' , 'date']
    scripdata.reset_index(inplace=True)
    scripdata.set_index(pd.DatetimeIndex(scripdata['date']), inplace=True)
    scripdata.drop('date', axis=1, inplace=True)
    print scripdata
    i += 1
    if i == 2:
        break

print scripinfo_dict
print scripdata.columns
