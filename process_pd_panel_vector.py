"""
In this approach, we transpose a given panel and select the list of `items`
that we are interested in using `vector` methods.

Intuitively this approach is fast one. But we have seen a radically different
behavior on different data sizes, so we want to be able to profile both
approaches separately and see why something seems more expensive.

"""
from __future__ import print_function

import time
import pandas as pd
from read_sql_data import get_hist_data_as_dataframes_dict
from tickerplot.sql.sqlalchemy_wrapper import get_metadata

metadata = get_metadata('sqlite:///nse_hist_data.sqlite3')

import cProfile, pstats, StringIO

max_limit = 40
limit = 20
while limit < max_limit:
    scripdata_dict = get_hist_data_as_dataframes_dict(metadata=metadata,
                                                        limit=limit)
    pan = pd.Panel(scripdata_dict)

    then0 = time.time()
    pr = cProfile.Profile()
    pr.enable()

    pan2 = pan.transpose(2, 0, 1)
    cl = pan2['close']
    cl2 = cl[cl.iloc[:, -1] > cl.iloc[:, -2]]
    pan11 = pan[cl2.index]

    pr.disable()
    pr.dump_stats('vector.stats')
    s = StringIO.StringIO()
    sort_by = 'cumulative'
    ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
    ps.print_stats(0.1)
    now0 = time.time()

    #print (limit, now0 - then0)
    #print (len(cl2.index))
    #print (s.getvalue())

    limit *= 2
