from pybit import WebSocket
from loguru import logger
import pandas as pd
import numpy as np


class WebsocketHandler:
    """
    Connector for Bybit's WebSocket API.
    """

    def __init__(self, endpoint, subscriptions):
        self.ws = WebSocket(
            endpoint,
            subscriptions=subscriptions,
            trim_data=False
        )
        self.lob = LOB()

    def fetch(self, topic, n=3):
        while n > 0:
            data_json = self.ws.fetch(topic)
            if data_json:
                if data_json.get('type') == 'snapshot':
                    n -= 1
                    logger.debug('got snapshot')
                    self.lob.fit_snapshot(data_json)
                elif data_json.get('type') == 'delta':
                    n -= 1
                    self.lob.fit_delta(data_json)
                    logger.debug('got delta')

        self.lob.save()


class LOB:
    """
    Limited Order Book
    """

    def __init__(self):
        self.lob_detail = np.empty([1, 18])
        self.lob_general = {}
        self.trade = None
        self.event_n = 0

    def fit_snapshot(self, j: "json"):
        data = j.get('data')
        seq = j.get('cross_seq')
        ts = j.get('timestamp_e6')
        # [order_id, side, price, size, lob_action, event_ts, sx_ts, rx_ts, order_type, order_executed, execution_price,
        # executed_size, agressor_side, matching_order_id, old_order_id, trade_id, size_ahead, orders_ahead]

        for record in data:
            temp = [record.get('id'), Util.d_buy_sell[record.get('side')], record.get('price'), record.get('size'),
                    np.nan, ts, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
            self.lob_detail = np.append(self.lob_detail, np.array(temp, ndmin=2), axis=0)

    def fit_delta(self, j: "json"):
        data = j.get('data')
        seq = j.get('cross_seq')
        ts = j.get('timestamp_e6')

        d = data.get('delete')
        if d:
            for record in d:
                _id = record.get('id')
                idx_to_delete = np.where(self.lob_detail[:, 0] == _id)
                if len(idx_to_delete) != 0:
                    self.lob_detail = np.delete(self.lob_detail, idx_to_delete, axis=0)
                else:
                    logger.debug("no record to delete")

        u = data.get('update')
        if u:
            for record in u:
                _id = record.get('id')
                idx_to_udpate = np.where(self.lob_detail[:, 0] == _id)
                if len(idx_to_udpate) != 0:
                    self.lob_detail[idx_to_udpate, 3] = record.get('size')
                else:
                    logger.debug("no record to update")

        i = data.get('insert')
        if i:
            for record in u:
                _id = record.get('id')
                idx_to_insert = np.where(self.lob_detail[:, 0] == _id)
                if len(idx_to_insert) == 0:
                    temp = [record.get('id'), Util.d_buy_sell[record.get('side')], record.get('price'),
                            record.get('size'), np.nan, ts, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
                            np.nan, np.nan, np.nan, np.nan, np.nan]
                    np.append(self.lob_detail, temp, axis=0)
                else:
                    logger.debug("record exists")

    def save(self):
        np.savetxt('./lob_detail.csv', self.lob_detail, delimiter=',', fmt='%s')


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

    handler = WebsocketHandler(endpoint=url, subscriptions=subs)
    handler.fetch(subs[0])
