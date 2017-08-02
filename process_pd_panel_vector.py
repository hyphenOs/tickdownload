"""
In this approach, we transpose a given panel and select the list of `items`
that we are interested in using `vector` methods.

Intuitively this approach is fast one. But we have seen a radically different
behavior on different data sizes, so we want to be able to profile both
approaches separately and see why something seems more expensive.

"""
import time
import pandas as pd
from read_sql_data import get_hist_data_as_dataframes_dict

import cProfile, pstats, StringIO

scripdata_dict = get_hist_data_as_dataframes_dict()
pan = pd.Panel(scripdata_dict)


then0 = time.time()

pr = cProfile.Profile()
pr.enable()

pan2 = pan.transpose(2, 0, 1)
cl = pan2['close']
cl2 = cl[cl.iloc[:, -1] > cl.iloc[:, -2]]
pan11 = pan[cl2.index]

pr.disable()
s = StringIO.StringIO()
sort_by = 'cumulative'
ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
ps.print_stats(0.1)

now0 = time.time()

print now0 - then0
print len(cl2.index)
print s.getvalue()
