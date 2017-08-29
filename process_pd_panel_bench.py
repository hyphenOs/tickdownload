"""
In this approach we are using `list comprehension` approach to filter data
based on certain criteria. Intuitively this should be slower than the
`vector` method.

We want to profile it for different datasets.
"""

from __future__ import print_function

import time
import pandas as pd
from read_sql_data import get_hist_data_as_dataframes_dict
from tickerplot.sql.sqlalchemy_wrapper import get_metadata

import cProfile, pstats, StringIO

import perf

class ProcessPandasPanelBench(object):

    def __init__(self, method='cProfile', limit_rows=0, db_path=None):
        self.db_path = db_path
        self.method_name = method
        self.limit_rows = limit_rows
        self.metadata = get_metadata(self.db_path)

    def panel_bench_lc(self, panel=None):
        sels = [panel[x]['close'][-1] > panel[x]['close'][-2] \
                    for x in panel]
        return sels

    def panel_bench_vector(self, panel=None):

        pan2 = panel.transpose(2, 0, 1)
        cl = pan2['close']
        cl2 = cl[cl.iloc[:, -1] > cl.iloc[:, -2]]
        pan11 = panel[cl2.index]

        return pan11.items

    def set_method(method_name='cProfile'):
        if method_name not in ('cprofile', 'perf'):
            raise ValueError("Method name should be one of 'cProfile', 'perf'")
        self.method_name = method_name

    def run_bench_cprofile(self, panel):

        # FIXME: Add a Contextanager Class
        then0 = time.time()
        pr = cProfile.Profile()
        pr.enable()

        selectors = self.panel_bench_lc(panel=panel)

        pr.disable()
        s = StringIO.StringIO()
        sort_by = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
        ps.print_stats(0.1)

        now0 = time.time()

        print (self.limit_rows, now0 - then0)
        print (len(filter(lambda x: x, selectors)))
        print (s.getvalue())

        # FIXME: Add a Contextanager Class
        then0 = time.time()
        pr = cProfile.Profile()
        pr.enable()

        selectors = self.panel_bench_vector(panel=panel)

        pr.disable()
        s = StringIO.StringIO()
        sort_by = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
        ps.print_stats(0.1)

        now0 = time.time()

        print (self.limit_rows, now0 - then0)
        print (len(filter(lambda x: x, selectors)))
        print (s.getvalue())

    def run_bench_perf(self, panel):

        r = perf.Runner(loops=1)
        r.bench_func("lc panel", self.panel_bench_lc, panel)

    def run_bench(self):

        # setup - common
        scripdata_dict = get_hist_data_as_dataframes_dict(
                                                metadata=self.metadata,
                                                limit=self.limit_rows)
        panel = pd.Panel(scripdata_dict)

        print (panel)
        if self.method_name == 'cProfile':
            self.run_bench_cprofile(panel)
        else:
            self.run_bench_perf(panel)


if __name__ == '__main__':
    #bench = ProcessPandasPanelBench(db_path='sqlite:///nse_hist_data_test2.sqlite3',
    #                                limit_rows=0)
    #bench.run_bench()
    print ("*"  * 80)
    limit_rows = 20
    while limit_rows <= 4000:
        bench2 = ProcessPandasPanelBench(db_path='sqlite:///nse_hist_data.sqlite3',
                                     limit_rows=limit_rows)
        bench2.run_bench()
        limit_rows *= 2
