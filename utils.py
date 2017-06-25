#
# Refer to LICENSE file and README file for licensing information.
#
# Generic Utils to be used by all
import os
import time
from datetime import datetime as dt
import sys

def get_datetime_for_datestr(datestr='2015-10-04', fmt='%Y-%m-%d'):
    """
    Returns a datetime.datetime object for a given string.

    This object can later be converted to whatever format as required.
    Everything is done with UTC timezone - to make sure if converted to
    Integer/Float the timestamp is correct.
    """

    # set tz to UTC
    if os.environ.has_key('TZ'):
        old_tz = os.environ['TZ']
    else:
        old_tz = None
    os.environ['TZ'] = ''
    time.tzset()

    # get the object now
    datetm = dt.strptime(datestr,fmt)

    # reset TZ
    if old_tz is not None:
        os.environ['TZ'] = old_tz
    else:
        os.environ.pop('TZ')
    time.tzset()

    return datetm

def get_datestr_from_ts(ts, fmt='%Y-%m-%d'):
    d = dt.utcfromtimestamp(ts)
    return time.strftime(fmt, d.timetuple())

# FIXME: Add proper logging wrapper
def log_debug(*args):
    return
    #print(args)

def log_info(*args):
    print(args)

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        sys.exit(0)

    date = sys.argv[1]
    fmt = '%Y-%m-%d'
    if len(sys.argv) == 3:
        fmt = sys.argv[2]

    print get_datestr_from_ts(time.mktime(time.strptime(date, fmt)), fmt)
    print get_datestr_from_ts(get_ts_for_datestr(date, fmt), fmt)
