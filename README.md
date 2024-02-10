This repository provides python code for Backtrader to work with capital.com.
Repository consists of 3 parts (bottom up):
1. capitalcom folder contains a python client for capital.com.
based on the work done by Iuri Campos https://pypi.org/project/python-capital/ (v0.10) and
Feite Brekeveld https://github.com/hootnot/oanda-api-v20/tree/master (v0.72). Adapted to suit my needs.

This also contains a script [capitalcom_markets_and_history.py] which can be used to retrieve historical data from capital.com
Either in Backtrader format or in ForexTester format. It also dumps the account information.

2. btcapitalcom folder which contains the necessary broker, feeds and store classes for Backtrader to work with capital.com

3. capitalcom_live and CapitalcomStrategy scripts are example scripts for Backtrader to test correct functioning of the integration.


Backtrader notes:
The capital.com Websocket API is used to get live data. This means TICK data is comming into backtrader.
This is a bit much to base a backtrader strategy on. 15 seconds and 30 seconds are supported by btcapitalcom as timeframes to handle in the strategy.

Even with a market order the capital.com API transitions from order (with an orderID) to a position (with a position ID).
To keep track and match order or position to a Backtrader ID the strategy uses the tradeid option (filled with a timestamp) for matching against capital.com transaction id's.
