#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# This package is based on the work done by Iuri Campos
# https://pypi.org/project/python-capital/ (v0.10) and
# Feite Brekeveld https://github.com/hootnot/oanda-api-v20/tree/master (v0.72)
#
# Modified by Jelle Bloemsma to fulfill the needs to work with Backtrader
# - passwords are sent encrypted
# - CandlesFactory added to enable fetching historic data beyond the max number
#   of candles in 1 web request
# - Example code added to be able to use the capatial com web service for realtime prices
# - possibility to use on live or demo account based on parameter when instantiating
#   the client
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
from enum import Enum
import requests
import json

from Cryptodome.Cipher import PKCS1_v1_5
from Cryptodome.PublicKey import RSA
import base64

#from websocket import create_connection
import websocket

class CapitalComConstants():
    HEADER_API_KEY_NAME = 'X-CAP-API-KEY'
    API_VERSION = 'v1'

    BASE_URL = 'https://demo-api-capital.backend-capital.com/api/{}/'.format(
        API_VERSION
    )

    WSS_URL = 'wss://api-streaming-capital.backend-capital.com/connect'

    SERVER_TIME_ENDPOINT = BASE_URL + 'time'
    PING_INFORMATION_ENDPOINT = BASE_URL + 'ping'

    SESSION_ENDPOINT = BASE_URL + 'session'
    ENCRYPTION_KEY_ENDPOINT = SESSION_ENDPOINT + '/' + 'encryptionKey'

    ACCOUNTS_ENDPOINT = BASE_URL + 'accounts'
    ACCOUNT_PREFERENCES_ENDPOINT = ACCOUNTS_ENDPOINT + '/' + 'preferences'

    ACCOUNT_HISTORY_ENDPOINT = BASE_URL + 'history'
    ACCOUNT_ACTIVITY_HISTORY_ENDPOINT = ACCOUNT_HISTORY_ENDPOINT + '/' + 'activity'
    ACCOUNT_TRANSACTION_HISTORY_ENDPOINT = ACCOUNT_HISTORY_ENDPOINT + '/' + 'transactions'

    ACCOUNT_ORDER_CONFIRMATION = BASE_URL + 'confirms'
    POSITIONS_ENDPOINT = BASE_URL + 'positions'
    ORDERS_ENDPOINT = BASE_URL + 'workingorders'

    MARKET_NAVIGATION_ENDPOINT = BASE_URL + 'marketnavigation'
    MARKET_INFORMATION_ENDPOINT = BASE_URL + 'markets'

    PRICES_INFORMATION_ENDPOINT = BASE_URL + 'prices'

    CLIENT_SENTIMENT_ENDPOINT = BASE_URL + 'clientsentiment'

    WATCHLISTS_ENDPOINT = BASE_URL + 'watchlists'


class DirectionType(Enum):
    BUY = 'BUY'
    SELL = 'SELL'

class OrderType(Enum):
    LIMIT = 'LIMIT'
    STOP = 'STOP'

class SourceType(Enum):
    CLOSE_OUT = 'CLOSE_OUT'
    DEALER = 'DEALER'
    SL = 'SL'
    SYSTEM = 'SYSTEM'
    TP = 'TP'
    USER = 'USER'

class StatusType(Enum):
    ACCEPTED = 'ACCEPTED'
    REJECTED = 'REJECTED'
    UNKNOWN = 'UNKNOWN'

class FilterType(Enum):
    EDIT_STOP_AND_LIMIT = 'EDIT_STOP_AND_LIMIT'
    POSITION = 'POSITION'
    SYSTEM = 'SYSTEM'
    WORKING_ORDER = 'WORKING_ORDER'

class TranslationType(Enum):
    DEPOSIT = 'DEPOSIT'
    WITHDRAWAL = 'WITHDRAWAL'
    REFUND = 'REFUND'
    WITHDRAWAL_MONEY_BACK = 'WITHDRAWAL_MONEY_BACK'
    TRADE = 'TRADE'
    SWAP = 'SWAP'
    TRADE_COMMISSION = 'TRADE_COMMISSION'
    TRADE_COMMISSION_GSL = 'TRADE_COMMISSION_GSL'
    CONVERT = 'CONVERT'
    NEGATIVE_BALANCE_PROTECTION = 'NEGATIVE_BALANCE_PROTECTION'
    REIMBURSEMENT = 'REIMBURSEMENT'
    TRADE_CORRECTION = 'TRADE_CORRECTION'
    CHARGEBACK = 'CHARGEBACK'
    ADJUSTMENT = 'ADJUSTMENT'
    DIVIDEND = 'DIVIDEND'
    ACCOUNT_CLOSURE = 'ACCOUNT_CLOSURE'
    BONUS = 'BONUS'
    TRANSFER = 'TRANSFER'

class Client():
    """
    This is API for market Capital.com
    list of endpoints here : https://open-api.capital.com
    API documantation here: https://capital.com/api-development-guide
    """

    """Starting session"""
    def __init__(self, log, pas, api_key, environment):
        self.login = log
        self.password = pas
        self.api_key = api_key
        self.environment = environment
        if self.environment == 'live':
            CapitalComConstants.BASE_URL = 'https://api-capital.backend-capital.com/api/{}/'.format(
                CapitalComConstants.API_VERSION
            )

        self.session = requests.Session()
        self.response = self.session.get(
            CapitalComConstants.ENCRYPTION_KEY_ENDPOINT,
            headers={'X-CAP-API-KEY': self.api_key}
        )
        _response = self.response.content.decode('utf-8')
        _response = json.loads(_response)
        _encryptionKey = _response['encryptionKey']
        timestamp = _response['timeStamp']

        _password = self.encryptPasswd(_encryptionKey, timestamp, pas)


        self.session = requests.Session()
        self.response = self.session.post(
            CapitalComConstants.SESSION_ENDPOINT,
            json={'identifier': self.login, 'password': _password, 'encryptedPassword': 'true'},
            headers={'X-CAP-API-KEY': self.api_key}
        )
        if self.response.status_code == 200:
            self.cst = self.response.headers['CST']
            self.x_security_token = self.response.headers['X-SECURITY-TOKEN']
        else:
            print ("Error occurred: ", self.response.content)




    """Encryption method"""
    @staticmethod
    def encryptPasswd(encryptionkey, timestamp, password):
        input = password + '|' + str(timestamp)
        input = base64.b64encode(str.encode(input))
        key = str.encode(encryptionkey)
        key = base64.b64decode(key)
        key = RSA.import_key(key)
        cipher = PKCS1_v1_5.new(key)

        ciphertext = bytes.decode(base64.b64encode(cipher.encrypt(input)))
        return ciphertext

    """Rest API Methods"""

    def _get(self, url, **kwargs):
        return requests.get(url, **kwargs)

    def _get_with_headers(self, url, **kwargs):
        return requests.get(url, **kwargs, headers=self._get_headers())

    def _get_with_params_and_headers(self, url, **kwargs):
        return requests.get(url, params=self._get_params(**kwargs), headers=self._get_headers())

    def _post(self, url, **kwargs):
        return requests.post(url, 
                                json=self._get_body_parameters(**kwargs),
                                headers=self._get_headers())

    def _delete(self, url, **kwargs):
        return requests.delete(url,
                            json=self._get_body_parameters(**kwargs),
                            headers=self._get_headers())

    def _put(self, url, **kwargs):
        return requests.put(url,
                            json=self._get_body_parameters(**kwargs),
                            headers=self._get_headers())

    """Headers"""
    def _get_headers(self, **kwargs):
        return {
                **kwargs,
                'CST': self.cst,
                'X-SECURITY-TOKEN': self.x_security_token
            }

    """Body Parameters"""
    def _get_body_parameters(self, **kwargs):
    
        return {
            **kwargs
            }

    """Params"""
    def _get_params(self, **kwargs):
        return {
            **kwargs
            }

    """ping"""
    def keepalive_ping(self):
        r = self._get_with_headers(
            CapitalComConstants.PING_INFORMATION_ENDPOINT,
        )
        return json.dumps(r.json(), indent=4)

    """SESSION"""
    def get_sesion_details(self): 
        """
        Returns the user's session details
        {
        "clientId": "12345678",
        "accountId": "12345678901234567",
        "timezoneOffset": 3,
        "locale": "en",
        "currency": "USD",
        "streamEndpoint": "wss://api-streaming-capital.backend-capital.com/"
        }
        """
        r = self._get_with_headers(
            CapitalComConstants.SESSION_ENDPOINT,
        )

        return json.dumps(r.json(), indent=4)


    def switch_account(self, accountId): 
        """
        Switch active account
        {
        "trailingStopsEnabled": false,
        "dealingEnabled": true,
        "hasActiveDemoAccounts": false,
        "hasActiveLiveAccounts": true
        }
        """
        r = self._put(
            CapitalComConstants.SESSION_ENDPOINT,
            accountId=accountId,
        )
        return json.dumps(r.json(), indent=4)
    
    def log_out_account(self):
        r = self._delete(
            CapitalComConstants.SESSION_ENDPOINT,
        )
        return json.dumps(r.json(), indent=4)
    
    """ACCOUNTS"""
    def all_accounts(self): 
        """
        {
        "accounts": [
        {
        "accountId": "12345678901234567",
        "accountName": "USD",
        "status": "ENABLED",
        "accountType": "CFD",
        "preferred": true,
        "balance": {
            "balance": 124.95,
            "deposit": 125.18,
            "profitLoss": -0.23,
            "available": 116.93
        },
        "currency": "USD"
        },
        {
        "accountId": "12345678907654321",
        "accountName": "Second account",
        "status": "ENABLED",
        "accountType": "CFD",
        "preferred": false,
        "balance": {
            "balance": 100,
            "deposit": 100,
            "profitLoss": 0,
            "available": 0
            },
        "currency": "USD"
         }
        ]
        }
        """
        r = self._get_with_headers(
            CapitalComConstants.ACCOUNTS_ENDPOINT,
        )
        if r.status_code != 200:
            print(r.reason)
        else:
            return json.dumps(r.json(), indent=4)


    def account_preferences(self): 
        r = self._get_with_headers(
            CapitalComConstants.ACCOUNT_PREFERENCES_ENDPOINT,
        )
        return json.dumps(r.json(), indent=4)


    def update_account_preferences(self, leverages: dict = None, hedgingmode: bool = None): 
        r = self._put(
            CapitalComConstants.ACCOUNT_PREFERENCES_ENDPOINT,
            leverages=leverages,
            hedgingMode=hedgingmode
        )
        return json.dumps(r.json(), indent=4)


    def account_activity_history(self, 
                                    fr: str, 
                                    to: str, 
                                    last_period: int = 600, 
                                    detailed: bool = True, 
                                    dealid: str = None, 
                                    epic: str = None, 
                                    filter: str = None): 
        
        f = 'from'
        r = self._get_with_params_and_headers(
            CapitalComConstants.ACCOUNT_ACTIVITY_HISTORY_ENDPOINT,
            f=fr,
            to=to,
            lastPeriod=last_period,
            detailed=detailed,
            dealId=dealid,
            epic=epic,
            filter=filter
        )
        return json.dumps(r.json(), indent=4)


    def account_transaction_history(self, 
                                    fr: str, 
                                    to: str, 
                                    last_period: int = 600, 
                                    type: TranslationType = None):

        r = self._get_with_params_and_headers(
            CapitalComConstants.ACCOUNT_TRANSACTION_HISTORY_ENDPOINT,
            f=fr,
            to=to,
            lastPeriod=last_period,
            type=type.value
        )
        return json.dumps(r.json(), indent=4)

    """MARKETS"""
    def market_categories(self):
        r = self._get_with_headers(
            CapitalComConstants.MARKET_NAVIGATION_ENDPOINT,
        )
        return json.dumps(r.json(), indent=4)

    def market_sub_nodes(self, nodeid: str):
        r = self._get_with_headers(
            CapitalComConstants.MARKET_NAVIGATION_ENDPOINT + '/' + nodeid,
        )
        return json.dumps(r.json(), indent=4)

    def market_details(self, epics: str):
        r = self._get_with_headers(
            CapitalComConstants.MARKET_INFORMATION_ENDPOINT + '?epics=' + epics,
        )
        return json.dumps(r.json(), indent=4)

    """PRICES"""
    def prices(self, epic, granularity, start_date, end_date, max):
        r = self._get_with_headers(
            CapitalComConstants.PRICES_INFORMATION_ENDPOINT + '/' + epic + '?' +
            'resolution=' + granularity +
            '&max=' + str(max) +
            '&from=' + start_date +
            '&to=' + end_date,
        )
        return json.dumps(r.json(), indent=4)

    """POSITIONS"""
    def position_order_confirmation(self, deal_reference: str):
        r = self._get_with_headers(
            CapitalComConstants.ACCOUNT_ORDER_CONFIRMATION + '/' + deal_reference,
        )
        return json.dumps(r.json(), indent=4)

    def all_positions(self):   
        r = self._get_with_headers(
            CapitalComConstants.POSITIONS_ENDPOINT,
        )
        if r.status_code != 200:
            print(r.reason)
        else:
            return json.dumps(r.json(), indent=4)

    def place_the_position(self, 
                            direction: DirectionType, 
                            epic: str,
                            type: str,
                            size: float, 
                            gsl: bool = False, 
                            tsl: bool = False, 
                            stop_level: float = None, 
                            stop_distance: float = None, 
                            stop_amount: float = None, 
                            profit_level: float = None, 
                            profit_distance: float = None, 
                            profit_amount: float = None):

        r = self._post(
            CapitalComConstants.POSITIONS_ENDPOINT,
            direction=direction.value,
            epic=epic,
            size=size,
            guaranteedStop=gsl,
            trailingStop=tsl,
            stopLevel=stop_level,
            stopDistance=stop_distance,
            stopAmount=stop_amount,
            profitLevel=profit_level,
            profitDistance=profit_distance,
            profitAmount=profit_amount
        )
        return json.dumps(r.json(), indent=4)


    def check_position(self, dealid: str):
        r = self._get_with_headers(
            CapitalComConstants.POSITIONS_ENDPOINT + '/' + dealid,
        )
        return json.dumps(r.json(), indent=4)


    def update_the_position(self, 
                            dealid: str,
                            gsl: bool = False, 
                            tsl: bool = False, 
                            stop_level: float = None, 
                            stop_distance: float = None, 
                            stop_amount: float = None, 
                            profit_level: float = None, 
                            profit_distance: float = None, 
                            profit_amount: float = None
                            ):
        r = self._put(
            CapitalComConstants.POSITIONS_ENDPOINT + '/' + dealid,
            guaranteedStop=gsl,
            trailingStop=tsl,
            stopLevel=stop_level,
            stopDistance=stop_distance,
            stopAmount=stop_amount,
            profitLevel=profit_level,
            profitDistance=profit_distance,
            profitAmount=profit_amount
        )
        return json.dumps(r.json(), indent=4)

    
    def close_position(self, dealid): 
        r = self._delete(
            CapitalComConstants.POSITIONS_ENDPOINT + '/' + dealid,
        )
        return json.dumps(r.json(), indent=4)

    """ORDER"""
    def all_orders(self):   
        r = self._get_with_headers(
            CapitalComConstants.ORDERS_ENDPOINT,
        )
        return json.dumps(r.json(), indent=4)


    def place_the_order(self, 
                            direction: DirectionType, 
                            epic: str, 
                            size: float, 
                            level: float,
                            type: OrderType,
                            gsl: bool = False, 
                            tsl: bool = False, 
                            good_till_date: str = None,
                            stop_level: float = None, 
                            stop_distance: float = None, 
                            stop_amount: float = None, 
                            profit_level: float = None, 
                            profit_distance: float = None, 
                            profit_amount: float = None):

        r = self._post(
            CapitalComConstants.ORDERS_ENDPOINT,
            direction=direction.value,
            epic=epic,
            size=size,
            level=level,
            type=type.value,
            goodTillDate=good_till_date,
            guaranteedStop=gsl,
            trailingStop=tsl,
            stopLevel=stop_level,
            stopDistance=stop_distance,
            stopAmount=stop_amount,
            profitLevel=profit_level,
            profitDistance=profit_distance,
            profitAmount=profit_amount
        )
        return json.dumps(r.json(), indent=4)

    
    def update_the_order(self, 
                            level: float = None,
                            good_till_date: str = None,
                            gsl: bool = False, 
                            tsl: bool = False, 
                            stop_level: float = None, 
                            stop_distance: float = None, 
                            stop_amount: float = None, 
                            profit_level: float = None, 
                            profit_distance: float = None, 
                            profit_amount: float = None):

        r = self._put(
            CapitalComConstants.ORDERS_ENDPOINT,
            level=level,
            goodTillDate=good_till_date,
            guaranteedStop=gsl,
            trailingStop=tsl,
            stopLevel=stop_level,
            stopDistance=stop_distance,
            stopAmount=stop_amount,
            profitLevel=profit_level,
            profitDistance=profit_distance,
            profitAmount=profit_amount
        )

        return json.dumps(r.json(), indent=4)
    
    
    def close_order(self, dealid): 
        r = self._delete(
            CapitalComConstants.ORDERS_ENDPOINT + '/' + dealid,
        )
        return json.dumps(r.json(), indent=4)

    def on_message(self, ws, message):
            msg = json.loads(message)
            print(msg)

    def on_error(self, ws, message):
            print(message)

    def on_open(self, ws):
            print('open')
            subscribe = {}
            payload = {}
            payload['epics'] = [self.epic]
            subscribe['destination'] = 'marketData.subscribe'
            subscribe['correlationId'] = '1'
            subscribe['cst'] = self.cst
            subscribe['securityToken'] = self.x_security_token
            subscribe['payload'] = payload
            subscribe = json.dumps(subscribe)
            self.ws.send(subscribe)

    def on_close(self, ws):
         print('closed')



    def start_streaming(self, epic):
        self.epic = epic
        subscribe = {}
        payload = {}
        payload['epics'] = ['BTCUSD']
        subscribe['destination'] = 'marketData.subscribe'
        subscribe['correlationId'] = '1'
        subscribe['cst'] = self.cst
        subscribe['securityToken'] = self.x_security_token
        subscribe['payload'] = payload
        subscribe = json.dumps(subscribe)
        self.ws = websocket.WebSocketApp(CapitalComConstants.WSS_URL,
                    on_message = lambda ws,msg: self.on_message(ws, msg),
                    on_error   = lambda ws,msg: self.on_error(ws, msg),
                    on_close   = lambda ws:     self.on_close(ws),
                    on_open    = lambda ws:     self.on_open(ws))
        self.ws.run_forever()










