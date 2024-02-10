import capitalcom.client
from capitalcom.contrib.factories import EpicCandlesFactory
import threading
import pandas as pd
import json
import time

#path to write the output .csv files to
path = "./data/"
#epics list of instruments to grab data for e.g. ['US100']
epics = ['US100']

#Resolution: possible values: MINUTE, MINUTE_5, MINUTE_15, MINUTE_30, HOUR, HOUR_4, DAY, WEEK
resolution = "MINUTE"

_from = "2024-2-5T08:00:00"

_to= "2024-2-7T19:00:00"

#Get the bid or ask price.
bidask = 'bid'

#Number of bars to retrieve in each partial call (max 1000 for Capital.com)
maxbars = 500

download = False
get_instruments = True
export_format_forextester = True

def ping_function():
    while sessionrunning:
        time.sleep(300)
        rv = CAPI.keepalive_ping()
        print("ping result: " + str(rv))
        if not sessionrunning:
            print("Finishing ping thread.")

def getnodes(nodeid):
    rv = CAPI.market_sub_nodes(nodeid)
    sub_nodes = json.loads(rv)
    return sub_nodes


with open("config_capitalcom.json", "r") as file:
    config = json.load(file)

apikey = config["capitalcom"]["apikey"]
account = config["capitalcom"]["account"]
password = config["capitalcom"]["password"]
environment = config["capitalcom"]["environment"]

CAPI = capitalcom.client.Client(account, password, apikey, environment)

sessionrunning = True
x = threading.Thread(target=ping_function, args=())
x.daemon = True
x.start()


#Write a csv file listing all accounts to the data directory
rv = CAPI.all_accounts()
accounts = pd.read_json(rv)
accounts.to_csv(path + 'capitalcom_accounts_' + environment + '.csv', index=False)


if get_instruments:
    result_list = []
    rv = CAPI.market_categories()
    nodes0 = json.loads(rv)

    for node0 in nodes0['nodes']:
        nodeid = (node0['id'])

        nodes1 = getnodes(nodeid)
        for node1 in nodes1['nodes']:
            nodes2 = getnodes(node1['id'])
            level_response = pd.json_normalize(node1)
            result_list.append(level_response)

            if 'markets' in nodes2:
                level_response = pd.json_normalize(nodes2, record_path=['markets'])
                if not level_response.empty:
                    result_list.append(level_response)
            else:
                level_response = pd.json_normalize(nodes2, record_path=['nodes'])
                if len(nodes2['nodes']) > 0:
                    for node2 in nodes2['nodes']:
                        nodes3 = getnodes(node2['id'])
                        'add a sleep timer to prevent hitting the 10 api calls per second limit'
                        time.sleep(0.100)
                        if len(nodes3) > 1:
                            level_response = pd.json_normalize(nodes3, record_path=['markets'])
                            result_list.append(level_response)

    pd.concat(result_list).to_csv(path + 'capitalcom_instruments.csv', index=False)


if download:
    params = {
       "resolution": resolution,
        "max": maxbars,
       "from": _from,
       "to": _to,
    }
    if export_format_forextester:
        for epic in epics:
            # The factory returns a generator generating consecutive
            # requests to retrieve full history from date '_from' till '_to'
            df = pd.DataFrame()
            for rv in EpicCandlesFactory(CAPI, epic=epic, params=params):
                #check if there is real candle data in the calls. We dont want to stop processing in case of a
                #{"errorCode":"error.prices.not-found"} which is thrown if part of the data is not available.
                if not "errorCode" in rv:
                    data = json.loads(rv)
                    results = [{"date": x['snapshotTimeUTC'][0:10].replace("-","."), "time": x['snapshotTimeUTC'][11:16],"open": float(x['openPrice'][bidask]), "high": float(x['highPrice'][bidask]),
                                "low": float(x['lowPrice'][bidask]), "close": float(x['closePrice'][bidask]), "volume": int(x['lastTradedVolume'])} for x in
                               data['prices']]
                if len(results) > 0:
                    tmp_df = pd.DataFrame(results)
                    #use df = df.append(tmp_df) for panda version < 2.0
                    df = pd.concat([df, tmp_df])
                    print("last candle: " + df['date'].iloc[-1] + " " + df['time'].iloc[-1])
    else:
        for epic in epics:
            # The factory returns a generator generating consecutive
            # requests to retrieve full history from date '_from' till '_to'
            df = pd.DataFrame()
            for rv in EpicCandlesFactory(CAPI, epic=epic, params=params):
                #check if there is real candle data in the calls. We dont want to stop processing in case of a
                #{"errorCode":"error.prices.not-found"} which is thrown if part of the data is not available.
                if not "errorCode" in rv:
                    data = json.loads(rv)
                    results = [{"time": x['snapshotTimeUTC'], "open": float(x['openPrice'][bidask]), "high": float(x['highPrice'][bidask]),
                                "low": float(x['lowPrice'][bidask]), "close": float(x['closePrice'][bidask]), "volume": float(x['lastTradedVolume'])} for x in
                               data['prices']]
                if len(results) > 0:
                    tmp_df = pd.DataFrame(results)
                    # use df = df.append(tmp_df) for panda version < 2.0
                    df = pd.concat([df, tmp_df])
                    print("last candle: " + df['time'].iloc[-1])

    print('writing: ' + path + 'capitalcom_' + epic + '_' + resolution + '.csv')
    df.to_csv(path + 'capitalcom_' + epic + '_' + resolution + '.csv', index=False)

