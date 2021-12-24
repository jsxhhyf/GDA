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


# what is atomic orders?

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

        self.msg_queue = []
        self.lock = threading.Lock()

        self.connector = threading.Thread(name='connector', target=lambda: self.ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
            # ping_payload='{"op":"ping"}'
        ))

        self.connector.daemon = True
        self.connector.start()

        self.consumer = threading.Thread(name='consumer', target=lambda: self.fetch(self.msg_queue))
        self.consumer.start()
        time.sleep(1)

    def subscribe(self, subs: list):
        self.ws.send(json.dumps({"op": "subscribe", "args": subs}))

    def _on_message(self, msg):
        # logger.debug(msg)
        self.lock.acquire()
        self.msg_queue.append(msg)
        self.lock.release()
        # logger.debug(f"msg stored! msg_list length: {len(self.msg_list)}")

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

    def cache_delta(self, msg_delta):
        self.delta_cache.append(msg_delta)

    def fetch(self, msg_list: list):
        while True:
            if msg_list:
                self.lock.acquire()
                msg_json = json.loads(msg_list.pop(0))
                # logger.debug(f'msg consumed! msg_list length: {len(self.msg_list)}')
                self.lock.release()
            else:
                continue

            if 'success' in msg_json:
                logger.info(f"Subscription of {msg_json.get('request').get('args')} is succeeded")
                continue

            if 'topic' in msg_json and 'orderBook' in msg_json.get('topic'):
                if msg_json.get('type') == 'snapshot':
                    # pass
                    self.lob.on_snapshot(msg_json)
                elif msg_json.get('type') == 'delta':
                    # self.delta_cache(msg_json)
                    # pass
                    # logger.debug(f"delta ts: {msg_json.get('timestamp_e6')}")
                    self.lob.on_delta(msg_json)
                    # self.lob.save()
            elif 'topic' in msg_json and 'trade' in msg_json.get('topic'):
                self.lob.on_trade(msg_json)


class LOB:
    """
    Limited Order Book
    """
    EVENT_NO = 1

    def __init__(self):
        self.lob_event = None
        self.delta_cache = []
        self.lob_general = {}
        self.trade = None
        self.event_n = 0

    def on_snapshot(self, j: "json"):

        self.reset_lob_event()

        _snap_array = []

        data = j.get('data')
        ts = j.get('timestamp_e6') // 1000
        # [event_no, order_id, side, price, size, lob_action, event_ts, order_type, order_cancelled, order_executed?, execution_price?,
        # executed_size, agressor_side, trade_id]

        for record in data:
            temp = [self.get_event_no(), record.get('id'), Util.d_buy_sell[record.get('side')], record.get('price'),
                    record.get('size'), np.nan, ts, Util.d_order_type['limit_orders'], np.nan, np.nan, np.nan, np.nan,
                    np.nan, np.nan]
            _snap_array.append(temp)

            ## create a dict of price, side, and size
            self.lob_general[record.get('price')] = [record.get('size'), record.get('side')]

        self.lob_event = np.array(_snap_array)

    def on_delta(self, j: json):
        # logger.debug(j.get('timestamp_e6'))
        self.cache_delta(j)

    def on_trade(self, j: "json"):

        logger.debug('in on_trade')
        # logger.debug(j.get('data')[0].get('trade_time_ms'))

        ready_event_list = []


        if self.trade:
            for delta in self.delta_cache:
                # for delete data
                for delta_record in delta.get('data').get('delete'):
                    logger.debug('delete')
                    # logger.debug(type(delta_record.get('price')))
                    # logger.debug(type(self.trade[0].get('price')))
                    logger.debug(delta.get('timestamp_e6'))
                    logger.debug(self.trade[0].get('trade_time_ms'))

                    ###
                    # for t in self.trade:
                    #     logger.debug(t.get('price'))
                    # logger.debug(delta_record.get('price'))
                    ###
                    trades = [t for t in self.trade if float(delta_record.get('price')) == float(t.get('price'))]
                    logger.debug(trades)
                    temp = [self.get_event_no(), delta_record.get('id'), Util.d_buy_sell[delta_record.get('side')],
                            delta_record.get('price'), self.lob_general[delta_record.get('price')][0], 3,
                            delta.get('timestamp_e6') // 1000, 1, 1, 0, np.nan,
                            np.nan, np.nan, np.nan]
                    if trades:
                        logger.debug(f'found trades in delete')
                        for t in trades:
                            if t.get('side') != delta_record.get('side'):
                                temp[Util.d_column_index['size']] -= t.get('size')

                                temp_market = [
                                    self.get_event_no(),
                                    temp[Util.d_column_index['order_id']],
                                    Util.d_buy_sell[t.get('side')],
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    1,
                                    t.get('trade_time_ms'),
                                    Util.d_order_type['market_orders'],
                                    0,
                                    1,
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    Util.d_buy_sell[t.get('side')],
                                    t.get('trade_id')
                                ]
                                ready_event_list.append(temp_market)

                                temp_limit = [
                                    self.get_event_no(),
                                    temp[Util.d_column_index['order_id']],
                                    3 - Util.d_buy_sell[t.get('side')],
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    1,
                                    t.get('trade_time_ms'),
                                    Util.d_order_type['limit_orders'],
                                    0,
                                    1,
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    Util.d_buy_sell[t.get('side')],
                                    t.get('trade_id')
                                ]
                                ready_event_list.append(temp_limit)

                    ready_event_list.append(temp)

                    # sync lob_general
                    self.lob_general[delta_record.get('price')][0] = 0

                    # logger.debug(f'len of temp {len(temp)}')

                # for update data
                for delta_record in delta.get('data').get('update'):
                    logger.debug('update')

                    ###
                    # for t in self.trade:
                    #     logger.debug(t.get('price'))
                    # logger.debug(delta_record.get('price'))
                    ###

                    trades = [t for t in self.trade if float(delta_record.get('price')) == float(t.get('price'))]
                    temp = [self.get_event_no(), delta_record.get('id'), Util.d_buy_sell[delta_record.get('side')],
                            delta_record.get('price'), delta_record.get('size'), 4,
                            delta.get('timestamp_e6') // 1000, 1, 0, np.nan, np.nan,
                            np.nan, np.nan, np.nan]

                    size_change = delta_record.get('size')

                    if trades:
                        logger.debug(f'found trades in update')

                        size_change -= self.lob_general[delta_record.get('price')][0]

                        for t in trades:
                            if t.get('side') != delta_record.get('side'):
                                size_change += t.get('size')

                                temp_market = [
                                    self.get_event_no(),
                                    temp[Util.d_column_index['order_id']],
                                    Util.d_buy_sell[t.get('side')],
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    1,
                                    t.get('trade_time_ms'),
                                    Util.d_order_type['market_orders'],
                                    0,
                                    1,
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    Util.d_buy_sell[t.get('side')],
                                    t.get('trade_id')
                                ]
                                ready_event_list.append(temp_market)

                                temp_limit = [
                                    self.get_event_no(),
                                    temp[Util.d_column_index['order_id']],
                                    3 - Util.d_buy_sell[t.get('side')],
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    1,
                                    t.get('trade_time_ms'),
                                    Util.d_order_type['limit_orders'],
                                    0,
                                    1,
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    Util.d_buy_sell[t.get('side')],
                                    t.get('trade_id')
                                ]
                                ready_event_list.append(temp_limit)

                    temp[Util.d_column_index['size']] = np.abs(size_change)
                    ready_event_list.append(temp)

                    # sync lob_general
                    self.lob_general[delta_record.get('price')][0] = delta_record.get('size')

                    # logger.debug(f'len of temp {len(temp)}')

                # for insert data
                for delta_record in delta.get('data').get('insert'):
                    logger.debug('insert')

                    ###
                    # for t in self.trade:
                    #     logger.debug(t.get('price'))
                    # logger.debug(delta_record.get('price'))
                    ###


                    trades = [t for t in self.trade if float(delta_record.get('price')) == float(t.get('price'))]
                    temp = [self.get_event_no(), delta_record.get('id'), Util.d_buy_sell[delta_record.get('side')],
                            delta_record.get('price'), delta_record.get('size'), 2,
                            delta.get('timestamp_e6') // 1000, 1, 0, 0, np.nan,
                            np.nan, np.nan, np.nan]
                    # sync lob_general
                    self.lob_general[delta_record.get('price')] = [temp[Util.d_column_index['size']],
                                                                   temp[Util.d_column_index['side']]]

                    if trades:
                        logger.debug(f'found trades in insert')

                        for t in trades:
                            if t.get('side') != delta_record.get('side'):
                                temp[Util.d_column_index['size']] += t.get('size')

                                temp_market = [
                                    self.get_event_no(),
                                    temp[Util.d_column_index['order_id']],
                                    Util.d_buy_sell[t.get('side')],
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    1,
                                    t.get('trade_time_ms'),
                                    Util.d_order_type['market_orders'],
                                    0,
                                    1,
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    Util.d_buy_sell[t.get('side')],
                                    t.get('trade_id')
                                ]
                                ready_event_list.append(temp_market)

                                temp_limit = [
                                    self.get_event_no(),
                                    temp[Util.d_column_index['order_id']],
                                    3 - Util.d_buy_sell[t.get('side')],
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    1,
                                    t.get('trade_time_ms'),
                                    Util.d_order_type['limit_orders'],
                                    0,
                                    1,
                                    temp[Util.d_column_index['price']],
                                    t.get('size'),
                                    Util.d_buy_sell[t.get('side')],
                                    t.get('trade_id')
                                ]
                                ready_event_list.append(temp_limit)

                    ready_event_list.append(temp)

                    # logger.debug(f'len of temp {len(temp)}')

            ready_event_ndarray = np.array(ready_event_list)
            # logger.debug(f'ready_event_list: {ready_event_list}')
            # logger.debug(f'ready_event_ndarray: {ready_event_ndarray}')
            # logger.debug(f'lob_event shape: {self.lob_event.shape}, ready_event shape: {ready_event_ndarray.shape}')
            self.lob_event = np.append(self.lob_event, ready_event_ndarray, axis=0)
            self.save()

            ### for market order book

            trades_temp_list = []
            for t in self.trade:
                temp = [int(float(t.get('price')) * 10e4), t.get('price'), t.get('trade_id')]

        else:  # if no trade come yet
            for delta in self.delta_cache:
                # for delete data
                for delta_record in delta.get('data').get('delete'):
                    temp = [self.get_event_no(), delta_record.get('id'), Util.d_buy_sell[delta_record.get('side')],
                            delta_record.get('price'), self.lob_general[delta_record.get('price')][0], 3,
                            delta.get('timestamp_e6') // 1000, 1, 1, 0, np.nan,
                            np.nan, np.nan, np.nan, np.nan]

                    # sync lob_general
                    self.lob_general[delta_record.get('price')][0] = 0

                # for update data
                for delta_record in delta.get('data').get('update'):
                    temp = [self.get_event_no(), delta_record.get('id'), Util.d_buy_sell[delta_record.get('side')],
                            delta_record.get('price'), delta_record.get('size'), 4,
                            delta.get('timestamp_e6') // 1000, 1, 0, np.nan, np.nan,
                            np.nan, np.nan, np.nan, np.nan]

                    # sync lob_general
                    self.lob_general[delta_record.get('price')][0] = delta_record.get('size')

                # for insert data
                for delta_record in delta.get('data').get('insert'):
                    temp = [self.get_event_no(), delta_record.get('id'), Util.d_buy_sell[delta_record.get('side')],
                            delta_record.get('price'), delta_record.get('size'), 2,
                            delta.get('timestamp_e6') // 1000, 1, 0, 0, np.nan,
                            np.nan, np.nan, np.nan, np.nan]
                    # sync lob_general
                    self.lob_general[delta_record.get('price')] = [temp[Util.d_column_index['size']],
                                                                   temp[Util.d_column_index['side']]]

        self.trade = j.get('data')
        self.clear_delta_cache()

    def cache_delta(self, delta: 'json'):
        self.delta_cache.append(delta)

    def clear_delta_cache(self):
        logger.debug("clearing delta_cache.")
        self.delta_cache.clear()

    def save(self):
        np.savetxt('./lob_events.csv', self.lob_event, delimiter=',', fmt='%s')

    def reset_lob_event(self):
        self.lob_event = np.empty([1, 18])

    @staticmethod
    def get_event_no():
        ret = LOB.EVENT_NO
        LOB.EVENT_NO += 1
        return ret


class Util:
    d_buy_sell = {'Unkonwn': 0, 'Buy': 1, 'Sell': 2, }
    d_action = {'unknown': 0, 'skip': 1, 'insert': 2, 'remove': 3, 'update': 4}
    d_order_type = {'unknown': 0, 'limit_orders': 1, 'market_orders': 2}
    d_column_index = {
        "event_no": 0,
        "order_id": 1,
        "side": 2,
        "price": 3,
        "size": 4,
        "lob_action": 5,
        "event_ts": 6,
        "order_type": 7,
        "order_cancelled": 8,
        "order_executed": 9,
        "execution_price": 10,
        "executed_size": 11,
        "agressor_side": 12,
        "trade_id": 13
    }

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
