from pybit import WebSocket

from loguru import logger

import pandas as pd
import numpy as np

# set up the initial dataframe
delta_df = pd.DataFrame(columns=[
    # 'quote_no',
    'event_no',
    'order_id',
    'original_order_id',
    'side',
    'price',
    'size',
    'lob_action',
    'event_timestamp',
    'send_timestamp',
    'receive_timestamp',
    'order_type',
    'order_executed',
    'execution_price',
    'executed_size',
    'agressor_side',
    'matching_order_id',
    'old_order_id',
    'trade_id',
    'size_ahead',
    'orders_ahead'
])


subs = [
    "orderBook_200.100ms.BTCUSD"
]

ws = WebSocket(
    "wss://stream-testnet.bybit.com/realtime",
    subscriptions=subs,
    trim_data=False
)
while True:
    data = ws.fetch(subs[0])
    if data:
        logger.debug(data)
        if data.get('type') == 'snapshot':
            pass
        elif data.get('type') == 'delta':
            break
    else:
        pass

print(delta_df)
