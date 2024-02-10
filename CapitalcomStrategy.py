import backtrader as bt
from datetime import datetime, timedelta




class CapitalcomTest(bt.Strategy):
    '''
    This strategy contains some additional methods that can be used to calcuate
    whether a position should be subject to a margin close out from Oanda.
    '''
    params = (('size', 0.1),('ma_fast', 20), ('ma_slow', 200), ('action_wait', 4), ('session_length', 60))


    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or datetime.combine(self.data.datetime.date(),self.data.datetime.time())
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.live_data = False
        self.session_end = datetime.utcnow() + timedelta(minutes=self.p.session_length)
        self.order = None
        self.lotsize = 1
        self.dataclose = self.datas[0].close
        self.dataclose1 = self.datas[1].close
        self.dt = datetime.utcnow()
        self.place_market_order = True
        self.place_limit_order = False
        self.close_market_order = False
        self.close_limit_order = False
        self.trade_id_market = None
        self.trade_id_limit = None
        self.market_order_bar = 0
        self.limit_order_bar = 0
        self.action_wait = self.p.action_wait

        self.total_value = 0
        self.margin = 0
        self.execution_price = 0
        self.execution_size = 0


        print('--------------------------------------------------')
        print('Strategy Created')
        print('--------------------------------------------------')

    def notify_store(self, msg, *args, **kwargs):
        print('*' * 5, 'STORE NOTIF:', msg)

    def notify_data(self, data, status, *args, **kwargs):
        print('*' * 5, 'DATA NOTIF:', data._getstatusname(status), *args)
        if data._getstatusname(status) == 'LIVE':
            self.live_data = True
        else:
            self.live_data = False

    def start(self):
        if self.data0.contractdetails is not None:
            print('-- Contract Details:')
            print(self.data0.contractdetails)
            self.lotsize = self.data0.contractdetails['instrument']['lotSize']


        header = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume',
                  'OpenInterest']
        print(', '.join(header))



    def next(self):

        txt = list()
        txt.append('Data0')
        txt.append('%04d' % len(self.data0))
        dtfmt = '%Y-%m-%dT%H:%M:%S.%f'
        txt.append('{:f}'.format(self.data.datetime[0]))
        txt.append('%s' % self.data.datetime.datetime(0).strftime(dtfmt))
        txt.append('{:f}'.format(self.data.open[0]))
        txt.append('{:f}'.format(self.data.high[0]))
        txt.append('{:f}'.format(self.data.low[0]))
        txt.append('{:f}'.format(self.data.close[0]))
        txt.append('{:6d}'.format(int(self.data.volume[0])))
        txt.append('{:d}'.format(int(self.data.openinterest[0])))
        print(', '.join(txt))

        txt = list()
        txt.append('Data1')
        txt.append('%04d' % len(self.data1))
        dtfmt = '%Y-%m-%dT%H:%M:%S.%f'
        txt.append('{:f}'.format(self.data1.datetime[0]))
        txt.append('%s' % self.data1.datetime.datetime(0).strftime(dtfmt))
        txt.append('{:f}'.format(self.data1.open[0]))
        txt.append('{:f}'.format(self.data1.high[0]))
        txt.append('{:f}'.format(self.data1.low[0]))
        txt.append('{:f}'.format(self.data1.close[0]))
        txt.append('{:6d}'.format(int(self.data1.volume[0])))
        txt.append('{:d}'.format(int(self.data1.openinterest[0])))
        print(', '.join(txt))

        if not self.live_data:
            #wait for history load before trading
            return

        if datetime.utcnow() > self.session_end:
            self.env.runstop()
            return

        if len(self) > self.action_wait:
            if self.place_market_order:
                #use timestamp to identify order
                self.trade_id_market = datetime.utcnow()
                self.order = self.buy(size=0.01, tradeid=self.trade_id_market)
                self.order.addinfo(name='Entry')
                self.market_order_bar = len(self)
                self.log("Market order bar: " + str(self.market_order_bar))
                self.place_market_order = False
                self.close_market_order = True
            if self.place_limit_order:
                #use timestamp to identify order
                self.trade_id_limit = datetime.utcnow()
                self.order = self.buy(exectype=bt.order.Order.Limit,
                                      size=0.01,
                                      price=self.data.close[0] * 0.98,
                                      valid=datetime.utcnow() + timedelta(days=1),
                                      tradeid=self.trade_id_limit)
                self.order.addinfo(name='Entry')
                self.limit_order_bar = len(self)
                self.log("Limit order bar: " + str(self.limit_order_bar))
                self.place_limit_order = False
                self.close_limit_order = True

        if self.close_market_order or self.close_limit_order:
            '''
            First check if margin close out conditions are met. If not, check
            to see if we should close the position through the strategy rules.

            If a close out occurs, we need to addinfo to the order so that we
            can log it properly later
            '''
            mco_result = self.check_mco(
                value=self.broker.getcash(),
                margin_used= self.broker.getcash() - self.broker.getvalue()
            )

            if mco_result:
                close = self.close()
                close.addinfo(name='MCO')

            if self.close_market_order:
                if len(self) > self.market_order_bar + self.p.action_wait:
                    self.close(tradeid=self.trade_id_market)
                    self.close_market_order = False
                    self.place_limit_order = True
                    self.action_wait = len(self) + self.p.action_wait

            if self.close_limit_order:
                if len(self) > self.limit_order_bar + self.p.action_wait:
                    self.broker.cancel(self.order)
                    self.place_market_order = True
                    self.close_limit_order = False
                    self.action_wait = len(self) + self.p.action_wait
        return

    def notify_trade(self, trade):
        if trade.isclosed:
            print('{}: Trade closed '.format(self.dt))
            print('{}: PnL Gross {}\n\n'.format(self.dt,
                                                round(trade.pnl,2)))

    def notify_order(self,order):
        if order.status in [order.Created, order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            self.log('Order Status: ' + str(order.Status[order.status]))
            return

        if order.status in [order.Completed, order.Partial]:
            self.log('Order Status: ' + str(order.Status[order.status]))
            self.execution_price = order.executed.price
            self.execution_size = order.executed.size
            self.total_value = self.execution_price * self.execution_size
            leverage = self.broker.getcommissioninfo(self.data).get_leverage()
            self.margin = abs((self.execution_price * self.execution_size) / leverage)

            if 'name' in order.info:
                if order.info['name'] == 'Entry':
                    print('{}: Entry Order Completed '.format(self.dt))
                    print('{}: Order Executed Price: {}, Executed Size {}'.format(self.dt,self.execution_price,
                                                                                  self.execution_size))
                    print('{}: Position Value: {} Margin Used: {}'.format(self.dt,round(self.margin * leverage,2),
                                                                          round(self.margin,2)))
                elif order.info['name'] == 'MCO':
                    print('{}: WARNING: Margin Close Out'.format(self.dt))
                else:
                    print('{}: Close Order Completed'.format(self.dt))


        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Status: ' + str(order.Status[order.status]))

        # Write down: no pending order
        self.order = None

    def check_mco(self, value, margin_used):
        '''
        Make a check to see if current used margin has reached 150% of available margin
        '''
        if margin_used == 0:
            return False
        elif (100 * value / margin_used) < 150:
            return True
        else:
            return False
