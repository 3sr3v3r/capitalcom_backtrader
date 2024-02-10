import re
import time
from datetime import datetime


def secs2time(e):
    """secs2time - convert epoch to datetime.

    >>> d = secs2time(1497499200)
    >>> d
    datetime.datetime(2017, 6, 15, 4, 0)
    >>> d.strftime("%Y%m%d-%H:%M:%S")
    '20170615-04:00:00'
    """
    w = time.gmtime(e)
    return datetime(*list(w)[0:6])


def granularity_to_time(s):
    """convert a named granularity into seconds.

    get value in seconds for named granularities: MINUTE, MINUTE_5, MINUTE_15, MINUTE_30, HOUR, HOUR_4, DAY, WEEK.

    >>> print(granularity_to_time("M5"))
    300
    """

    mfact = {
        'MINUTE': 60,
        'MINUTE_5': 300,
        'MINUTE_15': 900,
        'MINUTE_30': 1800,
        'HOUR': 3600,
        'HOUR_4': 14400,
        'DAY': 86400,
        'WEEK': 604800,
    }
    try:
        return mfact[s]

    except Exception as e:
        raise ValueError(e)
