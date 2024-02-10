#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# 2023: Jelle Bloemsma, backtrader feed functionality for Capital.com
# based on https://github.com/mementum/backtrader
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime, timedelta

from backtrader.feed import DataBase
from backtrader import TimeFrame, date2num, num2date
from backtrader.utils.py3 import (integer_types, queue, string_types,
                                  with_metaclass)
from backtrader.metabase import MetaParams
from btcapitalcom.stores import capitalcomstore


class MetaCapitalcomData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaCapitalcomData, cls).__init__(name, bases, dct)

        # Register with the store
        capitalcomstore.CapitalcomStore.DataCls = cls

class CapitalcomData(with_metaclass(MetaCapitalcomData, DataBase)):
    '''Capitalcom Data Feed.

    Params:

      - ``qcheck`` (default: ``0.5``)

        Time in seconds to wake up if no data is received to give a chance to
        resample/replay packets properly and pass notifications up the chain

      - ``historical`` (default: ``False``)

        If set to ``True`` the data feed will stop after doing the first
        download of data.

        The standard data feed parameters ``fromdate`` and ``todate`` will be
        used as reference.


      - ``backfill_start`` (default: ``True``)

        Perform backfilling at the start. fromdate will be used.

      - ``backfill`` (default: ``True``)

        Perform backfilling after a disconnection/reconnection cycle. The gap
        duration will be used to download the smallest possible amount of data

      - ``backfill_from`` (default: ``None``)

        An additional data source can be passed to do an initial layer of
        backfilling. Once the data source is depleted and if requested,
        backfilling from capital.com will take place. This is ideally meant to backfill
        from already stored sources like a file on disk, but not limited to.

      - ``bidask`` (default: ``True``)

        If ``True``, then the historical/backfilling requests will request
        bid/ask prices from the server

      - ``useask`` (default: ``False``)

        If ``True`` the *ask* part of the *bidask* prices will be used instead
        of the default use of *bid*

      - ``includeFirst`` (default: ``True``)

        Influence the delivery of the 1st bar of a historical/backfilling
        request by setting the parameter directly to the Capitalcom API calls

      - ``reconnect`` (default: ``True``)

        Reconnect when network connection is down

      - ``reconnections`` (default: ``-1``)

        Number of times to attempt reconnections: ``-1`` means forever

      - ``reconntimeout`` (default: ``5.0``)

        Time in seconds to wait in between reconnection attemps

    This data feed supports only this mapping of ``timeframe`` and
    ``compression``, which comply with the definitions in the CAPITALCOM API
    Developer's Guide::

        (bt.TimeFrame.Seconds, 15): 'SECONDS_15',
        (bt.TimeFrame.Seconds, 30): 'SECONDS_30',
        (bt.TimeFrame.Minutes, 1): 'MINUTE',
        (bt.TimeFrame.Minutes, 5): 'MINUTE_5',
        (bt.TimeFrame.Minutes, 15): 'MINUTE_15',
        (bt.TimeFrame.Minutes, 30): 'MINUTE_30',
        (bt.TimeFrame.Minutes, 60): 'HOUR',
        (bt.TimeFrame.Minutes, 240): 'HOUR_4',
        (bt.TimeFrame.Days, 1): 'DAY',
        (bt.TimeFrame.Weeks, 1): 'WEEK',

    Any other combination will be rejected
    '''
    params = (
        ('qcheck', 0.5),
        ('historical', False),  #only load historical data, then stop
        ('backfill_start', False),  # do backfilling at the start
        ('backfill', True),  # do backfilling when reconnecting
        ('backfill_from', None),  # additional data source to do backfill from
        ('useask', False),
        ('includeFirst', True),
        ('reconnect', True),
        ('reconnections', -1),  # forever
        ('reconntimeout', 5.0),
    )

    _store = capitalcomstore.CapitalcomStore

    # States for the Finite State Machine in _load
    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)

    _TOFFSET = timedelta()

    def _timeoffset(self):
        # Effective way to overcome the non-notification?
        return self._TOFFSET

    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        return True

    def __init__(self, **kwargs):
        self.o = self._store(**kwargs)
        self.lastTickdt = None
        self.notifDelayedSent = False
        self.leverage = kwargs.get('leverage', 1)

    def setenvironment(self, env):
        '''Receives an environment (cerebro) and passes it over to the store it
        belongs to'''
        super(CapitalcomData, self).setenvironment(env)
        env.addstore(self.o)

    def start(self):
        '''Starts the Capitalcom connection and gets the real contract and
        contractdetails if it exists'''
        super(CapitalcomData, self).start()

        # Create attributes as soon as possible
        self._statelivereconn = False  # if reconnecting in live state
        self._storedmsg = dict()  # keep pending live message (under None)
        self.qlive = queue.Queue()
        self._state = self._ST_OVER
        self.RFC3339 = "%Y-%m-%dT%H:%M:%S"

        # Kickstart store and get queue to wait on
        self.o.start(data=self)

        # check if the granularity is supported
        otf = self.o.get_granularity(self._timeframe, self._compression)
        if otf is None:
            self.put_notification(self.NOTSUPPORTED_TF)
            self._state = self._ST_OVER
            return

        self.contractdetails = cd = self.o.get_instrument(self.p.dataname)
        if cd is None:
            self.put_notification(self.NOTSUBSCRIBED)
            self._state = self._ST_OVER
            return

        if self.p.backfill_from is not None:
            self._state = self._ST_FROM
            self.p.backfill_from._start()
        else:
            self._start_finish()
            self._state = self._ST_START  # initial state for _load
            self._st_start()

        self._reconns = 0

    def _st_start(self, instart=True, tmout=None):
        if self.p.historical:
            if not self.notifDelayedSent:
                self.put_notification(self.DELAYED)
            dtend = None
            if self.todate < float('inf'):
                #dtend = datetime.strftime(datetime.utcnow(),self.RFC3339)
                dtend = datetime.strftime(num2date(self.todate), self.RFC3339)
            dtbegin = None
            if self.fromdate > float('-inf'):
                dtbegin = datetime.strftime(num2date(self.fromdate), self.RFC3339)


            self.qhist = self.o.candles(
                self.p.dataname, dtbegin, dtend,
                self._timeframe, self._compression)

            self._state = self._ST_HISTORBACK
            return True

        if (self.p.backfill_start and instart) or (not instart and self.p.backfill):
            if not self.notifDelayedSent:
                self.put_notification(self.DELAYED)

            dtend = datetime.strftime(datetime.utcnow(),self.RFC3339)
            dtbegin = None
            if self.fromdate > float('-inf') and self.p.backfill_start and instart:
                dtbegin = datetime.strftime(num2date(self.fromdate), self.RFC3339)
            elif not instart and self.p.backfill:
                dtbegin = datetime.strftime(self.lastTickdt, self.RFC3339)

            self.qhist = self.o.candles(
                self.p.dataname, dtbegin, dtend,
                self._timeframe, self._compression)
            if self.qhist != None:
                self._state = self._ST_HISTORBACK
            else:
                self.qlive.put(None)
                return False

        if self._state != self._ST_HISTORBACK and instart:
            self.qlive = self.o.streaming_prices(self.p.dataname, tmout=tmout)
        if instart:
            self._statelivereconn = self.p.backfill_start
        else:
            self._statelivereconn = self.p.backfill

        if self._statelivereconn:
            self.put_notification(self.DELAYED)
        if self._state != self._ST_HISTORBACK:
            self._state = self._ST_LIVE
            self._statelivereconn = True
        if instart:
            self._reconns = self.p.reconnections

        return True  # no return before - implicit continue

    def stop(self):
        '''Stops and tells the store to stop'''
        super(CapitalcomData, self).stop()
        self.o.stop()

    def haslivedata(self):
        return bool(self._storedmsg or self.qlive)  # do not return the objs

    def _load(self):
        if self._state == self._ST_OVER:
            return False

        while True:
            if self._state == self._ST_LIVE:
                try:
                    msg = (self._storedmsg.pop(None, None) or
                           self.qlive.get(timeout=self._qcheck))
                except queue.Empty:
                    return None  # indicate timeout situation

                if msg is None:  # Conn broken during historical/backfilling
                    if not self.notifDelayedSent:
                        self.put_notification(self.CONNBROKEN)
                        self.notifDelayedSent = True
                    # Try to reconnect
                    if not self.p.reconnect or self._reconns == 0:
                        # Can no longer reconnect
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # failed

                    self._reconns -= 1
                    self._st_start(instart=False, tmout=self.p.reconntimeout)
                    continue

                    if not self.p.reconnect or self._reconns == 0:
                        # Can no longer reconnect
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # failed

                    # Can reconnect
                    self._reconns -= 1
                    self._st_start(instart=False, tmout=self.p.reconntimeout)
                    continue

                self._reconns = self.p.reconnections

                # Process the message according to expected return type
                #if not self._statelivereconn:
                if self._statelivereconn:
                    if self._laststatus != self.LIVE:
                        if self.qlive.qsize() <= 1:  # very short live queue
                            self.put_notification(self.LIVE)
                    ret = self._load_tick(msg)
                    if ret:
                        return True

                    # could not load bar ... go and get new one
                    continue

                # Fall through to processing reconnect - try to backfill
                self._storedmsg[None] = msg  # keep the msg

            elif self._state == self._ST_HISTORBACK:
                msg = self.qhist.get()
                if msg is None:  # Conn broken during historical/backfilling
                    # Situation not managed. Simply bail out
                    self.put_notification(self.DISCONNECTED)
                    self._state = self._ST_OVER
                    return False  # error management cancelled the queue

                if msg:
                    if self._load_history(msg):
                        return True  # loading worked

                    continue  # not loaded ... date may have been seen
                else:
                    # End of histdata
                    if self.p.historical:  # only historical
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # end of historical
                    if self.p.backfill_start:
                        #start backfill completed
                        self.qlive = self.o.streaming_prices(self.p.dataname, tmout=None)
                        self._state = self._ST_LIVE
                        self.notifDelayedSent = False

                # Live is also wished - go for it
                self._state = self._ST_LIVE
                continue

            elif self._state == self._ST_FROM:
                if not self.p.backfill_from.next():
                    # additional data source is consumed
                    self._state = self._ST_START
                    continue

                # copy lines of the same name
                for alias in self.lines.getlinealiases():
                    lsrc = getattr(self.p.backfill_from.lines, alias)
                    ldst = getattr(self.lines, alias)

                    ldst[0] = lsrc[0]

                return True

            elif self._state == self._ST_START:
                if not self._st_start(instart=False):
                    self._state = self._ST_OVER
                    return False

    def _load_tick(self, msg):
        self.lastTickdt = datetime.utcfromtimestamp(int(msg['timestamp']) / 10 ** 3)
        dt = date2num(self.lastTickdt)
        if dt <= self.lines.datetime[-1]:
            return False  # time already seen

        # Common fields
        self.lines.datetime[0] = dt
        self.lines.volume[0] = 0.0
        self.lines.openinterest[0] = 0.0

        # Put the prices into the bar
        tick = float(msg['ofr']) if self.p.useask else float(msg['bid'])
        self.lines.open[0] = tick
        self.lines.high[0] = tick
        self.lines.low[0] = tick
        self.lines.close[0] = tick
        self.lines.volume[0] = 0.0
        self.lines.openinterest[0] = 0.0

        return True

    def _load_history(self, msg):
        dtobj = datetime.strptime(msg['snapshotTimeUTC'], self.RFC3339)
        dt = date2num(dtobj)
        if dt <= self.lines.datetime[-1]:
            return False  # time already seen

        # Common fields
        self.lines.datetime[0] = dt
        self.lines.volume[0] = float(msg['lastTradedVolume'])
        self.lines.openinterest[0] = 0.0

        # Put the prices into the bar
        if not self.p.useask:
            self.lines.open[0] = float(msg['openPrice']['bid'])
            self.lines.high[0] = float(msg['highPrice']['bid'])
            self.lines.low[0] = float(msg['lowPrice']['bid'])
            self.lines.close[0] = float(msg['closePrice']['bid'])
        else:
            self.lines.open[0] = float(msg['openPrice']['ask'])
            self.lines.high[0] = float(msg['highPrice']['ask'])
            self.lines.low[0] = float(msg['lowPrice']['ask'])
            self.lines.close[0] = float(msg['closePrice']['ask'])

        return True
