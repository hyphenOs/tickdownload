"""
In this approach we are using `list comprehension` approach to filter data
based on certain criteria. Intuitively this should be slower than the
`vector` method.

We want to profile it for different datasets.
"""

from __future__ import print_function

import time
import cProfile

import pandas as pd

from read_sql_data import get_hist_data_as_dataframes_dict
from tickerplot.sql.sqlalchemy_wrapper import get_metadata

metadata = get_metadata('sqlite:///nse_hist_data.sqlite3')


max_limit = 40
limit = 20
while limit < max_limit:
    scripdata_dict = get_hist_data_as_dataframes_dict(metadata=metadata,
                                                        limit=limit)
    pan = pd.Panel(scripdata_dict)

    then0 = time.time()
    pr = cProfile.Profile()
    pr.enable()

    sels = [pan[x]['close'][-1] > pan[x]['close'][-2] for x in pan]

    #pr.disable()
    #s = StringIO.StringIO()
    #sort_by = 'cumulative'
    #ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
    #ps.print_stats(0.1)

    now0 = time.time()

    print (limit, now0 - then0)
    print (len(sels))
    #print (s.getvalue())

    limit *= 2
