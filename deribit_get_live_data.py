import asyncio
import websockets
import json
import pandas as pd
from pandas.io.json import json_normalize

# To subscribe to this channel:
#exmaple msg setup
msg = \
    {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
        "channels": ["trades.option.BTC.raw"]}
    }

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
                data_norm = json_normalize(data['params']['data'])
                print(data_norm)
                print("\n")
                data_norm.to_csv('trade_logs.csv', header=None, mode='a')

asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))
