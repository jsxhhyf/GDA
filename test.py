from pybit import WebSocket
from loguru import logger
import pandas as pd
import numpy as np


class WebsocketHandler:
    """
    Connector for Bybit's WebSocket API.
    """

    def __init__(self, endpoint, subs):
        self.ws = WebSocket(
            endpoint,
            subscriptions=subs,
            trim_data=False
        )
        self.lob = LOB()

    def fetch(self, topic):
        data_json = self.ws.fetch(topic)
        if data_json.get('type') == 'snapshot':
            self.lob.fit_snapshot(data_json)
        elif data_json.get('type') == 'delta':


class LOB:
    """
    Limited Order Book
    """

    def __init__(self):
        self.lob_detail = None
        self.lob_general = {}
        self.trade = None
        self.event_n = 0

    def fit_snapshot(self, j: "json"):
        data = j.get('data')
        seq = j.get('cross_seq')
        ts = j.get('timestamp_e6')

        # [order_id, side, price, size, lob_action, event_ts, sx_ts, rx_ts, order_type, order_executed, execution_price,
        # executed_size, agressor_side, matching_order_id, old_order_id, trade_id, size_ahead, orders_ahead]

        temp = [data.get('id'), Util.d_buy_sell[data.get('side')], data.get('price'), data.get('size'), np.nan, ts,
                np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
        self.lob_detail = np.array(temp)

    def fit_delta(self, j: "json"):
        data = j.get('data')
        seq = j.get('cross_seq')
        ts = j.get('timestamp_e6')

        # [order_id, side, price, size, lob_action, event_ts, sx_ts, rx_ts, order_type, order_executed, execution_price,
        # executed_size, agressor_side, matching_order_id, old_order_id, trade_id, size_ahead, orders_ahead]

class Util:
    d_buy_sell = {'Buy': 1, 'Sell': 2, 'Unkonwn': 0}
    d_action = {'unknown': 0, 'skip': 1, 'insert': 2, 'remove': 3, 'update': 4}

    def __init__(self):
        pass


if __name__ == '__main__':

    url = "wss://stream-testnet.bybit.com/realtime"
    subs = [
        "orderBook_200.100ms.BTCUSD"
    ]

    handler = WebsocketHandler(endpoint=url, subs=subs)
    while True:
        handler.fetch()
