# -*- coding: utf-8 -*-

from datetime import datetime
import calendar
import logging

import capitalcom.client as prices
from capitalcom.contrib.generic import granularity_to_time, secs2time


logger = logging.getLogger(__name__)

MAX_BATCH = 1000
DEFAULT_BATCH = 100


def EpicCandlesFactory(CAPI, epic, params=None):
    """EpicCandlesFactory - generate EpicCandles requests.

    EpicCandlesFactory is used to retrieve historical data by
    automatically generating consecutive requests when the Capital.com limit
    of *count* records is exceeded.

    This is known by calculating the number of candles between *from* and
    *to*. If *to* is not specified *to* will be equal to *now*.

    The *count* parameter is only used to control the number of records to
    retrieve in a single request.

    The *includeFirst* parameter is forced to make sure that results do
    no have a 1-record gap between consecutive requests.

    Parameters
    ----------

    epic : string (required)
        the epic to create the order for

    params: params (optional)
        the parameters to specify the historical range, and the timeframe
        If no params are specified, just a single InstrumentsCandles request
        will be generated acting the same as if you had just created it
        directly.

    """
    RFC3339 = "%Y-%m-%dT%H:%M:%S"
    # if not specified use the default of 'MINUTE'
    gs = granularity_to_time(params.get('resolution', 'MINUTE'))

    _from = None
    _epoch_from = None
    if 'from' in params:
        _from = datetime.strptime(params.get('from'), RFC3339)
        _epoch_from = int(calendar.timegm(_from.timetuple()))

    _to = datetime.utcnow()
    if 'to' in params:
        _tmp = datetime.strptime(params.get('to'), RFC3339)
        # if specified datetime > now, we use 'now' instead
        if _tmp > _to:
            logger.info("datetime %s is in the future, will be set to 'now'",
                        params.get('to'))
            _to = datetime.strftime(datetime.utcnow(),self.RFC3339)
        else:
            _to = _tmp

    _epoch_to = int(calendar.timegm(_to.timetuple()))

    _count = params.get('max', DEFAULT_BATCH)

    if 'to' in params and 'from' not in params:
        raise ValueError("'to' specified without 'from'")

    if not params or 'from' not in params:
        yield instruments.InstrumentsCandles(instrument=instrument,
                                             params=params)

    else:
        delta = _epoch_to - _epoch_from
        nbars = delta / gs

        cpparams = params.copy()
        for k in ['count', 'from', 'to']:
            if k in cpparams:
                del cpparams[k]
        # force includeFirst
        cpparams.update({"includeFirst": True})

        # generate EpicCandles requests for all 'bars', each request
        # requesting max. count records
        for _ in range(_count, int(((nbars//_count)+1))*_count+1, _count):
            to = _epoch_from + _count * gs
            if to > _epoch_to:
                to = _epoch_to
            yparams = cpparams.copy()
            yparams.update({"from": secs2time(_epoch_from).strftime(RFC3339)})
            yparams.update({"to": secs2time(to).strftime(RFC3339)})
            yield CAPI.prices(epic, params['resolution'], secs2time(_epoch_from).strftime(RFC3339),
                              secs2time(to).strftime(RFC3339), _count)
            _epoch_from = to
