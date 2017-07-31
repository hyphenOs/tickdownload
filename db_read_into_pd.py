import sqlite3

import pandas as pd
import numpy as np

from sqlalchemy_wrapper import get_engine
from sqlalchemy_wrapper import execute_one
from sqlalchemy_wrapper import create_or_get_all_scrips_table
from sqlalchemy_wrapper import create_or_get_nse_equities_hist_data
from sqlalchemy_wrapper import select_expr

#scrips = pd.io.sql.read_sql('select distinct(name) from scrip_info;', con);

def get_all_scrips_names_in_db():
    all_scrips_table = create_or_get_all_scrips_table()
    scrips_select_st = select_expr([all_scrips_table.c.nse_symbol]).\
                                   where(all_scrips_table.c.nse_traded == True)


    result = execute_one(scrips_select_st)
    symbols = [row[0] for row in result.fetchall()]

    return symbols

import sys

lscrips = get_all_scrips_names_in_db()

# now we've to create a panel and add each 'script' as an axis to panel.

print (pd.__version__)

scripdata_dict = {}
i = 1

e = get_engine()
hist_data = create_or_get_nse_equities_hist_data()

for scrip in lscrips:
    sql_st = select_expr([hist_data.c.date,
                        hist_data.c.open, hist_data.c.high,
                        hist_data.c.low, hist_data.c.close,
                        hist_data.c.volume, hist_data.c.delivery]).\
                               where(hist_data.c.symbol == scrip).\
                                    order_by(hist_data.c.date)

    scripdata = pd.io.sql.read_sql(sql_st, e)

    scripdata.columns = ['date', 'open', 'high', 'low', 'close', 'volume',
                            'delivery']
    scripdata.reset_index(inplace=True)
    scripdata.set_index(pd.DatetimeIndex(scripdata['date']), inplace=True)
    scripdata.drop('date', axis=1, inplace=True)
    scripdata_dict[scrip] = scripdata

pan = pd.Panel(scripdata_dict)

import time

# Approach 1
then0 = time.time()

pan2 = pan.transpose(2, 0, 1)
cl = pan2['close']
cl2 = cl[cl.iloc[:, -1] > cl.iloc[:, -2]]
pan11 = pan[cl2.index]

now0 = time.time()

# Approach 2
then1 = time.time()

sels = [pan[pd]['close'][-1] > pan[pd]['close'][-2] for pd in pan]
pan1 = pan[sels]
sels2 = [(pan1[n]['close'][-1] >= (pan1[n]['close'][0] * 1.1)) for n in pan1]
pan2 = pan1[sels2]

now1 = time.time()

# Different calc

# Approach 1
then2 = time.time()

rets = [ pan[pd]['close'][-1] / pan[pd]['close'][0] for pd in pan]

now2 = time.time()

# Approach 2. FIXME : complete this
then3 = time.time()

pan2 = pan.transpose(2, 0, 1)
cl = pan2['close']
cl2 = cl[cl.iloc[:, -1] > cl.iloc[:, -2]]

now3 = time.time()


print now0 - then0

print now1 - then1

print now2 - then2

print now3 - then3
