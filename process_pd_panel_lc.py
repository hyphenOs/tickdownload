"""
In this approach we are using `list comprehension` approach to filter data
based on certain criteria. Intuitively this should be slower than the
`vector` method.

We want to profile it for different datasets.
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

sels = [pan[x]['close'][-1] > pan[x]['close'][-2] for x in pan]

pr.disable()
pr.dump_stats('lc.stats')
s = StringIO.StringIO()
sort_by = 'cumulative'
ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
ps.print_stats(0.1)

now0 = time.time()

print now0 - then0
print len(filter(lambda x: x, sels))
print s.getvalue()
