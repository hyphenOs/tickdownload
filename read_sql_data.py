import pandas as pd

from tickerplot.sql.sqlalchemy_wrapper import execute_one, get_engine
from tickerplot.sql.sqlalchemy_wrapper import create_or_get_all_scrips_table
from tickerplot.sql.sqlalchemy_wrapper import create_or_get_nse_equities_hist_data
from tickerplot.sql.sqlalchemy_wrapper import select_expr
from tickerplot.sql.sqlalchemy_wrapper import get_metadata
from sqlalchemy import desc
_DB_METADATA = None

def get_all_scrips_names_in_db(metadata=None):
    all_scrips_table = create_or_get_all_scrips_table(metadata=metadata)
    scrips_select_st = select_expr([all_scrips_table.c.nse_symbol]).\
                                   where(all_scrips_table.c.nse_traded == True)


    result = execute_one(scrips_select_st, engine=metadata.bind)
    symbols = [row[0] for row in result.fetchall()]

    return symbols

def get_hist_data_as_dataframes_dict(metadata=None, limit=0, max_scrips=16000):
    lscrips = get_all_scrips_names_in_db(metadata=metadata)

    e = metadata.bind
    hist_data = create_or_get_nse_equities_hist_data(metadata=metadata)

    scripdata_dict = {}
    scrips = 0
    for scrip in lscrips:
        sql_st = select_expr([hist_data.c.date,
                            hist_data.c.open, hist_data.c.high,
                            hist_data.c.low, hist_data.c.close,
                            hist_data.c.volume, hist_data.c.delivery]).\
                                where(hist_data.c.symbol == scrip).\
                                        order_by(desc(hist_data.c.date))

        if limit and type(limit) == int and limit > 0:
            sql_st = sql_st.limit(limit)

        scripdata = pd.io.sql.read_sql(sql_st, e)

        scripdata.columns = ['date', 'open', 'high', 'low', 'close', 'volume',
                            'delivery']
        scripdata.reset_index(inplace=True)
        scripdata.set_index(pd.DatetimeIndex(scripdata['date']), inplace=True)
        scripdata.drop('date', axis=1, inplace=True)
        scripdata_dict[scrip] = scripdata

        scrips += 1
        if scrips == max_scrips:
            break

    return scripdata_dict

def main(args):

    import argparse
    parser = argparse.ArgumentParser()

    # --dbpath option
    parser.add_argument("--dbpath",
                        help="Database URL to be used.",
                        dest="dbpath")

    args = parser.parse_args()

    # Make sure we can access the DB path if specified or else exit right here.
    if args.dbpath:
        try:
            global _DB_METADATA
            _DB_METADATA = get_metadata(args.dbpath)
        except Exception as e:
            print ("Not a valid DB URL: {} (Exception: {})".format(
                                                            args.dbpath, e))
            return -1

    get_hist_data_as_dataframes_dict()

    return 0

if __name__ == '__main__':

    import sys

    sys.exit(main(sys.argv))
