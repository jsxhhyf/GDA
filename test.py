import json
import sys
import threading
import time

import numpy as np
import websocket
from loguru import logger

logger.remove()
logger.add(sys.stderr,
           format='<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <cyan>{thread.name}</cyan> - <level>{message}</level>')


class WebsocketHandler:
    """
    Connector for Bybit's WebSocket API.
    """

    def __init__(self, url):
        self.ws = websocket.WebSocketApp(
            url=url,
            on_message=lambda ws, msg: self._on_message(msg),
            on_close=self._on_close,
            on_open=self._on_open,
            on_error=lambda ws, err: self._on_error(err)
        )
        self.lob = LOB()

        self.msg_list = []
        self.lock = threading.Lock()

        self.heartbeat = threading.Thread(name='heartbeat', target=lambda: self.ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
            # ping_payload='{"op":"ping"}'
        ))

        self.heartbeat.daemon = True
        self.heartbeat.start()

        self.consumer = threading.Thread(name='consumer', target=lambda: self.fetch(self.msg_list))
        self.consumer.start()
        time.sleep(0.5)

    def subscribe(self, subs: list):
        self.ws.send(json.dumps({"op": "subscribe", "args": subs}))

    def _on_message(self, msg):
        logger.debug(msg)
        self.lock.acquire()
        self.msg_list.append(msg)
        self.lock.release()
        logger.debug(f"msg stored! msg_list length: {len(self.msg_list)}")

    def _on_error(self, error):
        """
        Exit on errors and raise exception, or attempt reconnect.
        """
        pass

    def _on_open(self):
        """
        Log WS open.
        """
        logger.info(f'WebSocket opened.')

    def _on_close(self):
        """
        Log WS close.
        """
        logger.info(f'WebSocket closed.')

    def fetch(self, msg_list: list):
        while True:
            if msg_list:
                self.lock.acquire()
                msg_json = json.loads(msg_list.pop(0))
                self.lock.release()
            else:
                continue

            if 'success' in msg_json:
                logger.debug(f"Subscription of {msg_json.get('request').get('args')} is succeed")
                continue

            if 'topic' in msg_json and 'orderBook' in msg_json.get('topic'):
                if msg_json.get('type') == 'snapshot':
                    self.lob.fit_snapshot(msg_json)
                elif msg_json.get('type') == 'delta':
                    self.lob.fit_delta(msg_json)
            elif 'topic' in msg_json and 'trade' in msg_json.get('topic'):
                self.lob.fit_trade(msg_json)


class LOB:
    """
    Limited Order Book
    """

    def __init__(self):
        self.lob_delta = np.empty([1, 18])
        self.lob_general = {}
        self.trade = None
        self.event_n = 0

    def fit_snapshot(self, j: "json"):

        logger.debug("in fit_snapshot")
        return

        data = j.get('data')
        seq = j.get('cross_seq')
        ts = j.get('timestamp_e6')
        # [event_no, order_id, side, price, size, lob_action, event_ts, order_type, order_cancelled, order_executed, execution_price,
        # executed_size, agressor_side, trade_id]

        for record in data:
            temp = [record.get('id'), Util.d_buy_sell[record.get('side')], record.get('price'), record.get('size'),
                    np.nan, ts, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
            self.lob_delta = np.append(self.lob_delta, np.array(temp, ndmin=2), axis=0)

    def fit_delta(self, j: "json"):

        logger.debug('in fit_delta')
        return

        data = j.get('data')
        seq = j.get('cross_seq')
        ts = j.get('timestamp_e6')

        d = data.get('delete')
        if d:
            for record in d:
                _id = record.get('id')
                idx_to_delete = np.where(self.lob_delta[:, 0] == _id)
                if len(idx_to_delete) != 0:
                    self.lob_delta = np.delete(self.lob_delta, idx_to_delete, axis=0)
                else:
                    logger.debug("no record to delete")

        u = data.get('update')
        if u:
            for record in u:
                _id = record.get('id')
                idx_to_udpate = np.where(self.lob_delta[:, 0] == _id)
                if len(idx_to_udpate) != 0:
                    self.lob_delta[idx_to_udpate, 3] = record.get('size')
                else:
                    logger.debug("no record to update")

        i = data.get('insert')
        if i:
            for record in u:
                _id = record.get('id')
                idx_to_insert = np.where(self.lob_delta[:, 0] == _id)
                if len(idx_to_insert) == 0:
                    temp = [record.get('id'), Util.d_buy_sell[record.get('side')], record.get('price'),
                            record.get('size'), np.nan, ts, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
                            np.nan, np.nan, np.nan, np.nan, np.nan]
                    np.append(self.lob_delta, temp, axis=0)
                else:
                    logger.debug("record exists")

    def fit_trade(self, j: "json"):
        logger.debug('in fit_trade')

    def save(self):
        np.savetxt('./lob_detail.csv', self.lob_delta[1:, :], delimiter=',', fmt='%s')

    def reset_lob_detail(self):
        self.lob_delta = np.empty([1, 18])


class Util:
    d_buy_sell = {'Buy': 1, 'Sell': 2, 'Unkonwn': 0}
    d_action = {'unknown': 0, 'skip': 1, 'insert': 2, 'remove': 3, 'update': 4}

    def __init__(self):
        pass


if __name__ == '__main__':
    url = "wss://stream-testnet.bybit.com/realtime"
    subs = [
        "orderBook_200.100ms.BTCUSD",
        "trade.BTCUSD"
    ]

    handler = WebsocketHandler(url=url)
    handler.subscribe(subs)
