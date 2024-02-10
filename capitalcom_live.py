from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime
import backtrader as bt
from btplotting import BacktraderPlottingLive
from btplotting.schemes import Tradimo
from CapitalcomStrategy import CapitalcomTest
import btcapitalcom
import json

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(CapitalcomTest)

    #Load the broker account information from file
    path = "path to config_capitalcom.json"
    with open(path + "config_capitalcom.json", "r") as file:
        config = json.load(file)

    storekwargs = dict(
        apikey=config["capitalcom"]["apikey"],
        account=config["capitalcom"]["account"],
        accountID=config["capitalcom"]["accountID"],
        password=config["capitalcom"]["password"],
        environment=config["capitalcom"]["environment"],

        log_ticks=False,
        notif_transactions=True,
        stream_timeout=10,
    )

    capitalcomstore = btcapitalcom.stores.CapitalcomStore(**storekwargs)
    broker = capitalcomstore.getbroker()

    datakwargs = dict(
        timeframe=bt.TimeFrame.Ticks,
        compression=0,
        tz='UTC',
        backfill=True,
        backfill_start=True,
    )

    data = capitalcomstore.getdata(dataname="BTCUSD",
                                   fromdate=datetime(2024, 2, 9,9,0),
                                    **datakwargs)

    cerebro.resampledata(data, timeframe=bt.TimeFrame.Seconds, compression=15)
    cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes, compression=2)
    cerebro.setbroker(broker)
    # Set commission

    # Enable live plotting @http://localhost
    cerebro.addanalyzer(BacktraderPlottingLive, scheme=Tradimo(), lookback=120)

    cerebro.run()

    # Plot the result
    cerebro.plot()


