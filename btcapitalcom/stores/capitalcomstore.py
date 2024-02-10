#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# 2023: Jelle Bloemsma, backtrader store functionality for Capital.com
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

import collections
import copy
from datetime import datetime, timedelta, time
import time as _time
import json
import threading

import capitalcom.client
from capitalcom.contrib.factories import EpicCandlesFactory
import requests  # capitalcompy depdendency

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass
from backtrader.utils import AutoDict
import pandas as pd

import websocket


class Streamer():
    def __init__(self, STORE, q, cst, x_security_token, dataname, log_ticks):
        self.STORE = STORE
        self.q = q
        self.cst = cst
        self.x_security_token = x_security_token
        self.epics = dataname
        self.log_ticks = log_ticks
        self.ping = {'destination': 'ping',
                     'correlationId': 2,
                     'cst': self.cst,
                     'securityToken': self.x_security_token}
        self.ping = json.dumps(self.ping)

        try:
            self.ws = websocket.WebSocketApp('wss://api-streaming-capital.backend-capital.com/connect',
                        on_message = lambda ws,msg: self._on_message(ws, msg),
                        on_error   = lambda ws,msg: self._on_error(ws, msg),
                        on_close   = lambda ws:     self._on_close(ws),
                        on_open    = lambda ws:     self._on_open(ws))
            self.ws.run_forever()
            #self.ws.run_forever(ping_interval=50, ping_timeout=10)
        except Exception as e:
            self.put_notification(e)
            self.STORE.lost_connection = True
            return None


    def ping_webservice(self):
        while True:
            #ping more often (<1 min) when capital.com doesn't provide quotes which will kill the websocket.
            if time(20,45) < datetime.utcnow().time() < time(22,1):
                self.ws.send(self.ping)
                _time.sleep(45)
            else:
                self.ws.send(self.ping)
                _time.sleep(300)

    def _on_message(self, ws, message):
            msg = json.loads(message)
            if msg['status'] == 'OK':
                if msg['destination'] == 'quote':
                    self.q.put(msg['payload'])
                    if self.log_ticks:
                        print(msg)
                elif msg['destination'] == 'marketData.subscribe':
                    print("Subscribed to: " + str(msg['payload']['subscriptions']))
                    # start pinging the capital.com webservice
                    x = threading.Thread(target=self.ping_webservice, args=())
                    x.daemon = True
                    x.start()

                elif msg['destination'] == 'ping':
                    print("Broker webservice ping result: " + str(msg))
                else:
                    print('Websocket - unexpected message: ' + str(msg))
            else:
                print('Websocket - Unexpected Message: ' + str(msg))

    def _on_error(self, ws, message):
            print('Websocket - ERROR: ' + str(message))
            self.q.put(None)
            self.STORE.lost_connection = True

    def _on_open(self, ws):
            subscribe = {}
            payload = {}
            payload['epics'] = [self.epics]
            subscribe['destination'] = 'marketData.subscribe'
            subscribe['correlationId'] = '1'
            subscribe['cst'] = self.cst
            subscribe['securityToken'] = self.x_security_token
            subscribe['payload'] = payload
            subscribe = json.dumps(subscribe)
            self.ws.send(subscribe)

    def _on_close(self, ws):
         print('Websocket closed')

class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class CapitalcomStore(with_metaclass(MetaSingleton, object)):
    '''Singleton class wrapping to control the connections to Capitalcom.

    Params:

      - ``apikey`` (default:``None``): API access token

      - ``account`` (default: ``None``): account id

      - ``password`` (default: ``None``): account password

      - ``environment`` (default: ``demo``): use the test environment

      - ``accountID`` (default: ``None``): accountID if user(account) has multiple accounts

      - ``account_tmout`` (default: ``10.0``): refresh period for account
        value/cash refresh
    '''

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = dict(
        apikey='',
        account='',
        accountID='',
        password='',
        environment='demo',
        notif_transactions=True,
        stream_timeout=10,
        account_tmout=10.0,
        log_ticks=False
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(CapitalcomStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self._orders = collections.OrderedDict()  # map order.ref to oid
        self._ordersrev = collections.OrderedDict()  # map oid to order.ref
        self._transpend = collections.defaultdict(collections.deque)

        self.CAPI = capitalcom.client.Client(self.p.account, self.p.password, self.p.apikey, self.p.environment)
        self.CAPI.switch_account(self.p.accountID)
        self.RFC3339 = "%Y-%m-%dT%H:%M:%S"

        self.contractLotSize = 1
        self.leverage = 1
        self._cash = 0.0
        self._value = 0.0
        self._evt_acct = threading.Event()
        self.btcpositions = pd.DataFrame(columns = ['bt_oref','tradeid', 'size','executiontype',
                                                    'status', 'dealid', 'affectedDeals','monitor', 'dealreference'])
        self.monitor_orders = False
        self.lost_connection = False


    def start(self, data=None, broker=None):
        # Datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            if self.broker is not None:
                self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker
            self.broker_threads()

    def stop(self):
        # signal end of thread
        if self.broker is not None:
            self.q_ordercreate.put(None)
            self.q_orderclose.put(None)
            self.q_account.put(None)

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        '''Return the pending "store" notifications'''
        self.notifs.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifs.popleft, None)]

    # Capitalcom supported granularities
    _GRANULARITIES = {
        (bt.TimeFrame.Seconds, 5): 'SECONDS_5',
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

    }

    def get_positions(self):
        try:
            response = json.loads(self.CAPI.all_positions())
            pos = response["positions"]
        except Exception as e:
            self.put_notification(e)
            return None

        return pos

    def get_granularity(self, timeframe, compression):
        return self._GRANULARITIES.get((timeframe, compression), None)

    def get_instrument(self, dataname):
        try:
            response = json.loads(self.CAPI.market_details(dataname))
            inst = response["marketDetails"][0]

        except Exception as e:
            self.put_notification(e)
            return None

        return inst or None

    def candles(self, dataname, dtbegin, dtend, timeframe, compression):
        if not self.lost_connection:
            kwargs = locals().copy()
            kwargs.pop('self')
            kwargs['q'] = q = queue.Queue()
            t = threading.Thread(target=self._t_candles, kwargs=kwargs)
            t.daemon = True
            t.start()
            return q
        else:
            return None

    def _t_candles(self, dataname, dtbegin, dtend, timeframe, compression, q):

        granularity = self.get_granularity(timeframe, compression)
        if granularity is None:
            e = CapitalcomTimeFrameError()
            q.put(e.error_response)
            return

        _FAKE_HITORY = {'SECONDS_5': 5,
                  'SECONDS_15': 15,
                  'SECONDS_30': 30}

        if _FAKE_HITORY.get(granularity) != None:
            _granularity_org = granularity
            _step = _FAKE_HITORY.get(granularity)
            _generate_candles = True
            granularity = 'MINUTE'


        try:
            params = {
                "resolution": granularity,
                "max": 100,
                "from": dtbegin,
                "to": dtend,
            }

            for data in EpicCandlesFactory(self.CAPI, epic=dataname, params=params):
                #check if there is real candle data in the calls. We dont want to stop processing in case of a
                #{"errorCode":"error.prices.not-found"} which is thrown if part of the data is not available.
                batch = json.loads(data)
                if not "error" in data:
                    if not _generate_candles:
                        for candle in batch['prices']:
                            q.put(candle)
                    else:
                        for candle in batch['prices']:

                            for s in range(0, 59, _step):
                                generated_candle = copy.deepcopy(candle)
                                generated_candle['snapshotTime'] = datetime.strftime(
                                                                    (datetime.strptime(candle['snapshotTime'],
                                                                    self.RFC3339) + timedelta(0,s)), self.RFC3339)
                                generated_candle['snapshotTimeUTC'] = datetime.strftime(
                                                                    (datetime.strptime(candle['snapshotTimeUTC'],
                                                                    self.RFC3339) + timedelta(0,s)), self.RFC3339)
                                q.put(generated_candle)

                else:
                    self.put_notification("Error loading historical data" + data)

            q.put({})  # end of transmission

        except Exception as e:
            q.put(e)
            q.put(None)
            self.lost_connection = True
            return

    def keepalive_ping(self):
        while True:
            if not self.lost_connection:
                _time.sleep(180)
                try:
                    result = json.loads(self.CAPI.keepalive_ping())
                    print("Broker client ping result: " + result['status'])
                except Exception as e:
                    self.lost_connection = True
                    continue
            else:
                try:
                    self.CAPI = capitalcom.client.Client(self.p.account, self.p.password, self.p.apikey,
                                                         self.p.environment)

                    result = self.CAPI.keepalive_ping()
                    if 'OK' in result:
                        self.lost_connection = False
                except Exception as e:
                    _time.sleep(30)
                    continue


    def streaming_prices(self, dataname, tmout=None):
        q = queue.Queue()
        kwargs = {'q': q, 'dataname': dataname, 'tmout': tmout}
        t = threading.Thread(target=self._t_streaming_prices, kwargs=kwargs)
        t.daemon = True
        t.start()

        #start pinging the capital.com REST api
        x = threading.Thread(target=self.keepalive_ping, args=())
        x.daemon = True
        x.start()

        return q

    def _t_streaming_prices(self, dataname, q, tmout):
        if tmout is not None:
            _time.sleep(tmout)

        #Make leverage and contract lotSize settings available to store
        self.contractLotSize = self.datas[0].contractdetails['instrument']['lotSize']
        self.leverage = self.datas[0].leverage
        self.dataname = dataname

        streamer = Streamer(self,
                            q,
                            self.CAPI.cst,
                            self.CAPI.x_security_token,
                            dataname, self.p.log_ticks)


    def get_cash(self):
        return self._cash

    def get_value(self):
        return self._value

    _ORDEREXECS = {
        bt.Order.Limit: capitalcom.OrderType.LIMIT,
        bt.Order.Stop: capitalcom.OrderType.STOP,
        bt.Order.StopLimit: capitalcom.OrderType.STOP,
    }

    def broker_threads(self):
        self.q_account = queue.Queue()
        self.q_account.put(True)  # force an immediate update
        t = threading.Thread(target=self._t_account)
        t.daemon = True
        t.start()

        self.q_ordercreate = queue.Queue()
        t = threading.Thread(target=self._t_order_create)
        t.daemon = True
        t.start()

        self.q_positionclose = queue.Queue()
        t = threading.Thread(target=self._t_position_close)
        t.daemon = True
        t.start()

        self.q_orderclose = queue.Queue()
        t = threading.Thread(target=self._t_order_cancel)
        t.daemon = True
        t.start()

        self.q_ordermonitor = queue.Queue()
        t = threading.Thread(target=self._t_order_monitor)
        t.daemon = True
        t.start()

        # Wait once for the values to be set
        self._evt_acct.wait(self.p.account_tmout)

    def _t_account(self):
        while True:
            try:
                msg = self.q_account.get(timeout=self.p.account_tmout)
                if msg is None:
                    break  # end of thread
            except queue.Empty:  # tmout -> time to refresh
                pass

            try:
                rv = self.CAPI.all_accounts()
                allAccounts = json.loads(rv)
            except Exception as e:
                self.lost_connection = True
                self.put_notification(e)
                continue

            try:
               for account in allAccounts['accounts']:
                    if account['accountId'] == self.p.accountID:
                        self._cash = account['balance']['balance']
                        self._value = account['balance']['available']

            except KeyError:
                pass

            self._evt_acct.set()

    def order_create(self, order, stopside=None, takeside=None, **kwargs):
        #Check if the tradeid has been set
        if order.p.tradeid == 0:
            self.put_notification("Oder tradeid = 0. Rejected")
            self.broker._reject(order.ref)
            return

        #check if this order is actually meant to flatten an existing position
        deal = self.btcpositions.loc[self.btcpositions['tradeid'] == order.p.tradeid]
        if len(deal) == 1:
            dealid = deal.iloc[0,5]
            affectedDeals = json.loads(deal.iloc[0,6])
            for affectedDeal in affectedDeals:
                affectedDealId = affectedDeal['dealId']
                self.q_positionclose.put((order.ref, order.created.size, dealid, affectedDealId,))

        else:
            okwargs = dict()
            okwargs['epic'] = order.data._dataname
            okwargs['size'] = abs(order.created.size)
            okwargs['direction'] = capitalcom.DirectionType.BUY if order.isbuy() else capitalcom.DirectionType.SELL
            if order.exectype != bt.Order.Market:
                okwargs['level'] = order.created.price

            if order.valid:
                valid = order.data.num2date(order.valid)
                # To timestamp with seconds precision
                okwargs['good_till_date'] = datetime.strftime(valid, self.RFC3339)

            if order.exectype == bt.Order.Market:
                okwargs['type'] = '_MARKET'
            else:
                okwargs['type'] = self._ORDEREXECS[order.exectype]


            if order.exectype == bt.Order.StopLimit:
                self.put_notification("StopLimit not supported by Capital.com")
                return

            if order.exectype == bt.Order.StopTrail:
                okwargs['trailingStop'] = order.trailamount

            if stopside is not None:
                okwargs['stop_level'] = stopside.price

            if order.info.get('stop_level') is not None:
                okwargs['stop_level'] = order.info.get('stop_level')

            if takeside is not None:
                okwargs['profitLevel'] = takeside.price

            if order.info.get('profit_level') is not None:
                okwargs['profit_level'] = order.info.get('profit_level')

            okwargs.update(**kwargs)  # anything from the user

            #store the order information in the internal table
            #['Created', 'Submitted', 'Accepted', 'Partial', 'Completed', 'Canceled', 'Expired', 'Margin', 'Rejected']

            self.btcpositions = self.btcpositions.append(
                {'bt_oref': order.ref, 'tradeid': order.p.tradeid, 'size': order.size,
                 'executiontype': order.exectype, 'status': 'Created', 'dealid': '',
                 'affectedDeals': '', 'monitor': False, 'dealreference': ''}, ignore_index=True)

            self.q_ordercreate.put((order.ref, okwargs,))
            return order


    def _t_order_create(self):
        while True:
            msg = self.q_ordercreate.get()
            if msg is None:
                break

            oref, okwargs = msg

            if okwargs.get('type') == '_MARKET':
                try:
                    rv = self.CAPI.place_the_position(**okwargs)
                except Exception as e:
                    self.put_notification(e)
                    self.broker._reject(oref)
                    break
            else:
                try:
                    rv = self.CAPI.place_the_order(**okwargs)
                except Exception as e:
                    self.put_notification(e)
                    self.broker._reject(oref)
                    return

            # Get the DealId that is used for future actions
            try:
                o = json.loads(rv)
                dealReference = o['dealReference']
                rvc = self.CAPI.position_order_confirmation(dealReference)
                conf = json.loads(rvc)
                dealId = conf['dealId']
                affectedDeals = json.dumps(conf['affectedDeals'])

            except Exception as e:
                self.put_notification(e)
                self.broker._reject(oref)
                break

            self._orders[oref] = dealId
            self.broker._submit(oref)
            self.broker._accept(oref)
            self.btcpositions.loc[self.btcpositions['bt_oref'] == oref, 'dealid'] = dealId
            self.btcpositions.loc[self.btcpositions['bt_oref'] == oref, 'affectedDeals'] = affectedDeals

            if okwargs.get('type') == '_MARKET':
                self.btcpositions.loc[self.btcpositions['bt_oref'] == oref, 'status'] = 'Position'
                if conf['status'] == 'OPEN':
                    if conf['direction'] == 'SELL':
                        size = -1 * conf['size']
                    else:
                        size = conf['size']
                    self.broker._fill(oref,size,conf['level'],'ORDER_FILLED')
            else:
                self.btcpositions.loc[self.btcpositions['bt_oref'] == oref, 'status'] = 'Accepted'
                self.btcpositions.loc[self.btcpositions['bt_oref'] == oref, 'monitor'] = True
                self.monitor_orders = True

    def order_cancel(self, order):
        print(self.btcpositions.to_markdown())
        deal = self.btcpositions.loc[self.btcpositions['tradeid'] == order.p.tradeid]
        if len(deal) == 1:
            dealid = deal.iloc[0,5]
            affectedDeals = json.loads(deal.iloc[0,6])
            self.broker._accept(order.ref)
            for affectedDeal in affectedDeals:
                affectedDealId = affectedDeal['dealId']
                self.q_orderclose.put((order.ref, dealid, affectedDealId,))
        return order

    def _t_order_cancel(self):
        while True:
            oref, dealid, affectedDealId  = self.q_orderclose.get()
            if oref is None:
                break

            try:
                o = self.CAPI.close_order(affectedDealId)
            except Exception as e:
                break  # not cancelled - FIXME: notify

            self.btcpositions.drop(self.btcpositions[self.btcpositions.dealid == dealid].index, inplace=True)
            self.monitor_orders = False
            self.broker._cancel(oref)

    def _t_position_close(self):
        while True:
            msg = self.q_positionclose.get()
            if msg is None:
                break

            try:
                oref, size, dealid, affectedDealId = msg
                rvp = self.CAPI.close_position(affectedDealId)
                if 'error' in rvp or rvp == '':
                    self.btcpositions.drop(self.btcpositions[self.btcpositions.dealid == dealid].index,
                                           inplace=True)
                    self.put_notification(rvp)
                    self.broker._reject(oref)
            except Exception as e:
                self.put_notification(e)
                break

            self.broker._submit(oref)
            self.broker._accept(oref)  # taken immediately

            try:
                result = json.loads(rvp)
                dealReference = result['dealReference']
                rvc = self.CAPI.position_order_confirmation(dealReference)
                conf = json.loads(rvc)
            except Exception as e:
                self.put_notification(e)
                self.broker._reject(oref)
                break

            if conf['status'] == 'CLOSED':
                if conf['direction'] == 'SELL':
                    size = -1 * conf['size']
                else:
                    size = conf['size']
                self.broker._fill(oref, size, conf['level'], 'ORDER_FILLED')
                self.btcpositions.drop(self.btcpositions[self.btcpositions.dealid == dealid].index, inplace = True)


    def _t_order_monitor(self):
        #periodically validate status of limit / stop orders and resulting positions
        while True:
            if not self.monitor_orders:
                #print('order monitor sleeping for  180 secs')
                _time.sleep(180)
            else:
                #Check if pending order(s) have been filled:
                positions = self.CAPI.all_positions()
                if '[]' not in positions:
                    positions = json.loads(positions)
                    if len(positions['positions']) != 0:
                        monitored_orders = self.btcpositions.loc[(self.btcpositions['monitor'] == True) &
                                                                 (self.btcpositions['status'] == 'Accepted')]
                        if len(monitored_orders.index) != 0:
                            #iterate to df rows. use for loop to preserve naming and data types
                            for i in range(0, len(monitored_orders.index)):
                                for position in positions['positions']:
                                    if position['position']['workingOrderId'] == monitored_orders['dealid'][i]:
                                        self.btcpositions.loc[self.btcpositions['dealid'] == monitored_orders['dealid'][i],
                                        'dealid'] = position['position']['dealId']
                                        self.btcpositions.loc[self.btcpositions['dealid'] == position['position']['dealId'],
                                        'status'] = 'Position'
                                        self.btcpositions.loc[self.btcpositions['dealid'] == position['position']['dealId'],
                                        'dealreference'] = position['position']['dealReference']
                                        self.broker._fill(monitored_orders['bt_oref'][i], monitored_orders['size'][i],
                                                          position['position']['level'], 'ORDER_FILLED')

                #check if any of the monitored positions have been closed because of SL /TP
                monitored_positions = self.btcpositions.loc[(self.btcpositions['monitor'] == True) &
                                                         (self.btcpositions['status'] == 'Position')]
                if len(monitored_positions.index) != 0:
                    for i in range(0, len(monitored_positions.index)):
                        dealreference = monitored_positions['dealreference'][i]
                        rvc = self.CAPI.position_order_confirmation(dealreference)
                        if 'error' in rvc:
                            break
                        confirmation = json.loads(rvc)
                        if confirmation['status'] == 'CLOSED':
                            self.btcpositions.drop(
                                self.btcpositions[self.btcpositions.dealreference == dealreference].index,
                                inplace=True)
                if (len(self.btcpositions.loc[(self.btcpositions['monitor'] == True)].index) == 0):
                    self.monitor_orders = False
                #print('order monitor sleeping for 30 secs')
                _time.sleep(30)
