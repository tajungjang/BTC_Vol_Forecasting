import asyncio
import websockets
import json
import pandas as pd

# To subscribe to this channel:
#exmaple msg setup
msg = \
    {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
        "channels": ["markprice.options.btc_usd"]}
    }

async def call_api(msg):
   async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
       await websocket.send(msg)
       while websocket.open:
           response = await websocket.recv()
           data = response
           data = pd.io.json.json_normalize(response)
           #data = pd.DataFrame(data['result']).set_index('instrument_name')
           print(data)
           print("\n\n")
           # do something with the notifications...


asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))
