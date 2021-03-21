import pymysql
import math
import scipy.stats as stat
from scipy.stats import norm
import pandas as pd
from pandas.io.json import json_normalize
import websockets
import numpy as np
import urllib.request
import json
import sqlalchemy
#from sqlalchemy.ext.asyncio import create_async_engine
import asyncio
import tormysql

#-----------------------------------------------------------------#
#msg to send to deribit to subscribe to option trades websocket#
#-----------------------------------------------------------------#

msg = \
    {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
        "channels": ["trades.option.BTC.raw"]}
    }

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#connection for tormysql(async ver of pymysql)                    #
#-----------------------------------------------------------------#

pool = tormysql.ConnectionPool(
   max_connections = 20, #max open connections
   idle_seconds = 7200, #conntion idle timeout time, 0 is not timeout
   wait_connection_timeout = 3, #wait connection timeout
   host = "deribit-db-instance.csduabs5w5go.us-east-1.rds.amazonaws.com",
   port=3306,
   user = "admin",
   passwd = "Deribit123",
   db = "deribit",
   #ssl='../../Downloads/deribit-key-pair.pem',
   charset = "utf8"
)

loop = asyncio.get_event_loop()

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#get dataframe from table from the aws mysql rds db               #
#-----------------------------------------------------------------#

async def get_sql_df():
    async with await pool.Connection() as conn:
       async with conn.cursor() as cursor:
           query = "SELECT *\
                   FROM deribit.option_table\
                   WHERE str_to_date(substring(instrument_name, locate('-',instrument_name)+1,locate('-', instrument_name, locate('-',instrument_name)+1) - locate('-',instrument_name)-1), '%d%b%y') >= CURDATE();"
           await cursor.execute(query)
           df = pd.read_sql(query, conn)
    await pool.close()
    return df

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#bucket data by instrument_name                                   #
#-----------------------------------------------------------------#

def get_df(df):
    df = df.filter(['instrument_name', 'direction', 'amount'])
    df['direction'] = df['direction'].replace('buy', 1)
    df['direction'] = df['direction'].replace('sell', -1)
    df = df.assign(net_amount = lambda x: (x['amount'] * x['direction']))
    aggr_df = df.groupby(['instrument_name'], sort=False).agg({'net_amount': sum})
    aggr_df.reset_index(level=0, inplace=True)
    return aggr_df
#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#get greeks for each of the instruments                           #
#-----------------------------------------------------------------#

def get_greeks(active_trades):
    greeks = pd.DataFrame()
    count = 0
    for i in active_trades['instrument_name']:
        if(count < 20):
            count += 1
            greeks = get_orderbook_data(greeks, i)
        else:
            asyncio.sleep(1)
            count = 0
    columns = greeks.loc[:, ['underlying_price','underlying_index', 'open_interest','interest_rate', 'mark_iv', 'greeks']]
    greeks = pd.DataFrame(columns['greeks'].values.tolist())
    columns = columns.drop(['greeks'], axis = 1)
    greeks = greeks.join(columns)
    greeks['vanna'] = np.nan
    return greeks

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#get aggregated gex of all instruments                            #
#-----------------------------------------------------------------#

def get_gex(active_trades, active_trades_greeks):
    total_gamma = active_trades['net_amount'] * active_trades_greeks['gamma']
    return sum(total_gamma)
#-----------------------------------------------------------------#


#-----------------------------------------------------------------#
#get vanna for each of the instruments                           #
#-----------------------------------------------------------------#

def get_vanna(active_trades, greeks):
    #get days till expiration from active trades and add as a series
    dte = pd.to_datetime(active_trades['instrument_name'].apply(lambda x: x.split('-')[1]))-pd.Timestamp.today()
    strike = active_trades['instrument_name'].apply(lambda x: x.split('-')[2]).astype(int)
    active_trades['strike'] = strike
    #active_trades.astype({'strike': 'int32'}).dtypes
    active_trades['dte'] = dte.dt.days
    sigmaT = greeks['mark_iv'] * active_trades['dte'] ** 0.5
    d1 = (math.log(greeks['underlying_price'] / active_trades['strike']) + \
                   (greeks['interest_rate'] * 0.5 * (greeks['mark_iv'] ** 2)) \
                   * active_trades['dte']) / sigmaT
    d2 = d1 - sigmaT
    vannas = []
    #we'd like to vectorize this but d1 norm.pdf(d1) is not trivial due to some stupid type error
    #I can't be bothered to figure out right now
    for i in range(active_trades.shape[0]):
        vannas.append(0.01 * -math.e ** active_trades['dte'][i] * d2[i] / greeks['mark_iv'][i] * norm.pdf(d1[i]))
    return vannas
    #[0.01 * -e ** (-self.q * self.T) * self.d2 / self.sigma * norm.pdf(self.d1)]

#-----------------------------------------------------------------#
#get aggregated vex of all instruments                            #
#-----------------------------------------------------------------#

def get_vex(active_trades, greeks):
    vanna = get_vanna(active_trades, greeks)
    return vanna * no_stale['net_amount']

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#post new trade + updated gex/vex value to aws db                 #
#-----------------------------------------------------------------#

async def post_to_db(new_trade, greeks):
    new_trade['gex'] = get_gex(no_stale, greeks)
    new_trade['vex'] = get_vex(no_stale, greeks)
    #in the script, new_trade will be a df instead oa series so this line should
    #be removed
    df_to_post = new_trade.to_frame()
    df_to_post = df_to_post.transpose()
    sqlUrl = sqlalchemy.engine.url.URL(
        drivername="mysql+pymysql",
        username='admin',
        password='Deribit123',
        host='deribit-db-instance.csduabs5w5go.us-east-1.rds.amazonaws.com',
        port=3306,
        database='deribit',
        #make this the server's path and also make it secure?
        query={"ssl_key": "/home/tjang/Downloads/deribit-key-pair.pem"},
    )
    #engine = create_async_engine(sqlUrl)
    engine = sqlalchemy.create_engine(sqlUrl)
    df_to_post.to_sql('option_table', engine, if_exists='append', index=False)

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
# what to do every time we receive a trade                        #
#-----------------------------------------------------------------#

async def bulk_post(new_trade):
    db_df = await get_sql_df()
    db_df.append(new_trade, ignore_index=True)
    get_df(db_df)
    greeeks = get_greeks(db_df)
    await post_to_db(db_df, greeks)

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#connect and subscribe to deribit options websocket               #
#-----------------------------------------------------------------#

async def call_api(msg):
    async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
        await websocket.send(msg)
        while websocket.open:
            response = await websocket.recv()
            data = response
            data = json.loads(data)
            if('result' in data):
                pass
            else:
                print(json_normalize(data['params']['data']))
                await bulk_post(json_normalize(data['params']['data']))

#-----------------------------------------------------------------#

#-----------------------------------------------------------------#

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))
