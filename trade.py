from pybit import WebSocket

from loguru import logger

import pandas as pd
import numpy as np

# set up the initial dataframe

trade_df = pd.DataFrame(columns=[
    'order_id',
    'price',
    'trade_id',
    'timestamp',
    'side',
    'msg_orginal_type'
])

subs = [
    'trade.BTCUSD'
]

ws = WebSocket(
    "wss://stream.bybit.com/realtime",
    subscriptions=subs,
)
n = 0
while True:
    data = ws.fetch_alpha(subs[0])

    if data:
        if data.get('success'):
            continue
        else:
            if n > 10000:
                break
            # logger.debug(data)
            if n % 100 == 0:
                logger.debug(n)
            n += 1
            _msg_orginal_type = 'trade'
            for d in data.get('data'):
                _timestamp = d.get('trade_time_ms')
                _price = d.get('price')
                # _order_id = d.get('id')
                _trade_id = d.get('trade_id')
                _side = d.get('side')
                _size = d.get('size')
                trade_df = trade_df.append(
                    {'order_id': np.nan, 'price': _price, 'trade_id': _trade_id, 'timestamp': _timestamp, 'side': _side,
                     'msg_orginal_type': _msg_orginal_type}, ignore_index=True)
    else:
        pass

trade_df.to_csv('./trade.csv')
