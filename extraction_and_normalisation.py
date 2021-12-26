import json
import sys
import threading
import time

import numpy as np
import websocket
from loguru import logger

logger.remove()
logger.add(sys.stderr,
           format='<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function:<20}</cyan>:<cyan>{line:<3}</cyan> | <cyan>{thread.name}</cyan> - <level>{message}</level>')


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
        self.util = Util()
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

    def fetch(self, msg_list: list):

        while True:
            if msg_list:
                self.lock.acquire()
                msg_json = json.loads(msg_list.pop(0))
                # logger.debug('msg consumed! msg_list length: {}', len(msg_list))
                self.lock.release()
            else:
                continue

            if 'success' in msg_json:
                logger.info(f"Subscription of {msg_json.get('request').get('args')} is succeeded")
                continue

            if 'topic' in msg_json and 'orderBook' in msg_json.get('topic'):

                if msg_json.get('type') == 'snapshot':
                    self.lob.on_snapshot(msg_json)
                elif msg_json.get('type') == 'delta':
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
        self.util = Util()

        self.lob_event = None
        self.delta_cache = []
        self.lob_general = {}
        self.trade = None
        self.event_n = 0
        self.market_order_book = None

    def on_snapshot(self, j: "json"):
        self.debug_on_receive('snapshot', j)
        self.reset_lob_event()

        _snap_array = []

        data = j.get('data')
        ts = j.get('timestamp_e6') // 1000

        for record in data:
            temp = [self.get_event_no(), record.get('id'), Util.d_buy_sell[record.get('side')], record.get('price'),
                    record.get('size'), np.nan, ts, Util.d_order_type['limit_orders'], np.nan, np.nan, np.nan, np.nan,
                    np.nan, np.nan]
            _snap_array.append(temp)

            # create a dict of price, side, and size
            self.lob_general[record.get('price')] = [record.get('size'), record.get('side')]

        self.lob_event = np.array(_snap_array)

    def on_delta(self, j: json):
        self.debug_on_receive('delta', j)
        self.cache_delta(j)

    def on_trade(self, j: "json"):

        self.debug_on_receive('trade', j)

        ready_event_list = []

        if self.delta_cache:

            for delta in self.delta_cache:

                # for delete data
                delta_delete = delta['data']['delete']

                if delta_delete:

                    logger.debug('DELETE delta detected!')

                    for delta_record in delta_delete:
                        trades = [t for t in self.trade['data'] if float(delta_record['price']) == float(t['price'])]

                        ready_event_list.append(self.handle_delete(delta_record, trades))

                # for update data
                delta_update = delta['data']['update']

                if delta_update:

                    logger.debug('UPDATE delta detected!')

                    for delta_record in delta['data']['update']:
                        trades = [t for t in self.trade['data'] if float(delta_record['price']) == float(t['price'])]

                        ready_event_list.append(self.handle_update(delta_record, trades))

                # for insert data
                delta_insert = delta['data']['insert']

                if delta_insert:

                    logger.debug('INSERT delta detected!')

                    for delta_record in delta.get('data').get('insert'):
                        trades = [t for t in self.trade if float(delta['price']) == float(t['price'])]

                        ready_event_list.append(self.handle_insert(delta_record, trades))

        else:
            logger.debug('trade arrives earlier than delta')

        ready_event_ndarray = np.array(ready_event_list)
        self.lob_event = np.append(self.lob_event, ready_event_ndarray, axis=0)

        ## for market order book

        trades_temp_list = []
        # logger.debug(self.trade)
        for t in self.trade:
            temp = [int(float(t.get('price')) * 1e5), t.get('price'), t.get('trade_id'), t.get('trade_time_ms'),
                    t.get('side'), t.get('size')]
            trades_temp_list.append(temp)

            # logger.debug(np.array(trades_temp_list).shape)
            # logger.debug(np.array(trades_temp_list))

        if self.market_order_book is not None:
            self.market_order_book = np.append(self.market_order_book, np.array(trades_temp_list), axis=0)
        else:
            self.market_order_book = np.array(trades_temp_list)

        self.save()

        self.trade = j

    def handle_delete(self, delta: 'json', trades: list = None):

        event_list = []

        temp = [self.get_event_no(), delta['id'], Util.d_buy_sell[delta['side']], delta['price'],
                self.lob_general[delta['price']][0], 3, int(delta['timestamp_e6'] // 1e3), 1, 1, 0, np.nan, np.nan,
                np.nan, np.nan]

        if trades:

            logger.debug('found trades for delete {} @ {}', delta['price'], delta['timestamp_e6'])
            logger.debug('delta_record: {}', delta)
            logger.debug('trade found: {}', trades)

            for t in trades:

                if t['side'] != delta['side']:
                    temp[Util.d_column_index['size']] -= t['size']

                    # generated market order
                    temp_market = [
                        self.get_event_no(),  # event_no
                        temp[Util.d_column_index['order_id']],  # order_id
                        Util.d_buy_sell[t['side']],  # side
                        temp[Util.d_column_index['price']],  # price
                        t['size'],  # size
                        1,  # lob_action
                        t['trade_time_ms'],  # event_ts
                        Util.d_order_type['market_orders'],  # order_type
                        0,  # order_cancelled
                        1,  # order_executed
                        temp[Util.d_column_index['price']],  # execution_price
                        t['size'],  # execution_size
                        Util.d_buy_sell[t['side']],  # agressor_side
                        t['trade_id']  # trade_id
                    ]
                    event_list.append(temp_market)

                    # corresponding limit order
                    temp_limit = [
                        self.get_event_no(),  # event_no
                        temp[Util.d_column_index['order_id']],  # order_id
                        3 - Util.d_buy_sell[t['side']],  # side
                        temp[Util.d_column_index['price']],  # price
                        t['size'],  # size
                        1,  # lob_action
                        t['trade_time_ms'],  # event_ts
                        Util.d_order_type['limit_orders'],  # order_type
                        0,  # order_cancelled
                        1,  # order_executed
                        temp[Util.d_column_index['price']],  # execution_price
                        t['size'],  # execution_size
                        Util.d_buy_sell[t['side']],  # agressor_side
                        t['trade_id']  # trade_id
                    ]
                    event_list.append(temp_limit)
                else:
                    logger.warning('SAME SIDE DETECTED IN DELETE DELTA!')
                    logger.debug('trade: {}', t)
                    logger.debug('delta: {}', delta)

                event_list.append(temp)

        else:
            logger.debug('No matching trade found for delete {} @ {}', delta['price'], delta['timestamp_e6'])

        # sync lob_general
        self.lob_general[delta['price']][0] = 0

        return event_list

    def handle_update(self, delta: 'json', trades: list = None):
        event_list = []

        temp = [self.get_event_no(), delta['id'], Util.d_buy_sell[delta['side']], delta['price'],
                self.lob_general[delta['price']][0], 4, int(delta['timestamp_e6'] // 1e3), 1, 1, 0, np.nan, np.nan,
                np.nan, np.nan]

        if trades:

            logger.debug('found trades for update {} @ {}', delta['price'], delta['timestamp_e6'])
            logger.debug('delta_record: {}', delta)
            logger.debug('trade found: {}', trades)

            for t in trades:

                size_change = delta['size']
                size_change -= self.lob_general[delta['price']][0]

                if t['side'] != delta['side']:

                    size_change += t.get('size')

                    # generated market order
                    temp_market = [
                        self.get_event_no(),  # event_no
                        temp[Util.d_column_index['order_id']],  # order_id
                        Util.d_buy_sell[t['side']],  # side
                        temp[Util.d_column_index['price']],  # price
                        t['size'],  # size
                        1,  # lob_action
                        t['trade_time_ms'],  # event_ts
                        Util.d_order_type['market_orders'],  # order_type
                        0,  # order_cancelled
                        1,  # order_executed
                        temp[Util.d_column_index['price']],  # execution_price
                        t['size'],  # execution_size
                        Util.d_buy_sell[t['side']],  # agressor_side
                        t['trade_id']  # trade_id
                    ]
                    event_list.append(temp_market)

                    # corresponding limit order
                    temp_limit = [
                        self.get_event_no(),  # event_no
                        temp[Util.d_column_index['order_id']],  # order_id
                        3 - Util.d_buy_sell[t['side']],  # side
                        temp[Util.d_column_index['price']],  # price
                        t['size'],  # size
                        1,  # lob_action
                        t['trade_time_ms'],  # event_ts
                        Util.d_order_type['limit_orders'],  # order_type
                        0,  # order_cancelled
                        1,  # order_executed
                        temp[Util.d_column_index['price']],  # execution_price
                        t['size'],  # execution_size
                        Util.d_buy_sell[t['side']],  # agressor_side
                        t['trade_id']  # trade_id
                    ]
                    event_list.append(temp_limit)
                else:
                    logger.warning('SAME SIDE DETECTED IN UPDATE DELTA!')
                    logger.debug('trade: {}', t)
                    logger.debug('delta: {}', delta)

                temp[Util.d_column_index['size']] = np.abs(size_change)
                event_list.append(temp)

        else:
            logger.debug('No matching trade found for update {} @ {}', delta['price'], delta['timestamp_e6'])

        # sync lob_general
        self.lob_general[delta['price']][0] = delta['size']

        return event_list

    def handle_insert(self, delta: 'json', trades: list = None):

        event_list = []

        temp = [self.get_event_no(), delta['id'], Util.d_buy_sell[delta['side']], delta['price'],
                self.lob_general[delta['price']][0], 2, int(delta['timestamp_e6'] // 1e3), 1, 0, 0, np.nan, np.nan,
                np.nan, np.nan]

        if trades:

            logger.debug('found trades for insert {} @ {}', delta['price'], delta['timestamp_e6'])
            logger.debug('delta_record: {}', delta)
            logger.debug('trade found: {}', trades)

            # sync lob_general
            self.lob_general[delta['price']] = [temp[Util.d_column_index['size']],
                                                           temp[Util.d_column_index['side']]]

            for t in trades:

                if t['side'] != delta['side']:
                    temp[Util.d_column_index['size']] += t['size']

                    # generated market order
                    temp_market = [
                        self.get_event_no(),  # event_no
                        temp[Util.d_column_index['order_id']],  # order_id
                        Util.d_buy_sell[t['side']],  # side
                        temp[Util.d_column_index['price']],  # price
                        t['size'],  # size
                        1,  # lob_action
                        t['trade_time_ms'],  # event_ts
                        Util.d_order_type['market_orders'],  # order_type
                        0,  # order_cancelled
                        1,  # order_executed
                        temp[Util.d_column_index['price']],  # execution_price
                        t['size'],  # execution_size
                        Util.d_buy_sell[t['side']],  # agressor_side
                        t['trade_id']  # trade_id
                    ]
                    event_list.append(temp_market)

                    # corresponding limit order
                    temp_limit = [
                        self.get_event_no(),  # event_no
                        temp[Util.d_column_index['order_id']],  # order_id
                        3 - Util.d_buy_sell[t['side']],  # side
                        temp[Util.d_column_index['price']],  # price
                        t['size'],  # size
                        1,  # lob_action
                        t['trade_time_ms'],  # event_ts
                        Util.d_order_type['limit_orders'],  # order_type
                        0,  # order_cancelled
                        1,  # order_executed
                        temp[Util.d_column_index['price']],  # execution_price
                        t['size'],  # execution_size
                        Util.d_buy_sell[t['side']],  # agressor_side
                        t['trade_id']  # trade_id
                    ]
                    event_list.append(temp_limit)
                else:
                    logger.warning('SAME SIDE DETECTED IN DELETE DELTA!')
                    logger.debug('trade: {}', t)
                    logger.debug('delta: {}', delta)

                event_list.append(temp)

        else:
            logger.debug('No matching trade found for delete {} @ {}', delta['price'], delta['timestamp_e6'])

        return event_list


    def cache_delta(self, delta: 'json'):
        logger.debug('Adding to delta_cache ... ')
        self.delta_cache.append(delta)
        logger.debug('Now there are {} delta records in delta_cache', len(self.delta_cache))

    def clear_delta_cache(self):
        logger.debug("clearing delta_cache.")
        self.delta_cache.clear()

    def save(self):
        np.savetxt('./lob_events.csv', self.lob_event, delimiter=',', fmt='%s')
        np.savetxt('./market_order_book.csv', self.market_order_book, delimiter=',', fmt='%s')
        logger.debug(self.market_order_book.shape)

    def reset_lob_event(self):
        self.lob_event = np.empty([1, 18])

    def get_timestamp(self, t: str, j: 'json') -> list:

        ts_list = []

        if t == 'trade':
            for data in j['data']:
                ts_list.append(data['trade_time_ms'])

        elif t == 'delta' or t == 'snapshot':
            ts_list.append(int(j['timestamp_e6'] // 1e3))

        return ts_list

    def debug_on_receive(self, t: str, j: 'json'):
        logger.debug('{:<10s} packet received! ts: {}', t.upper(), self.get_timestamp(t, j))

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

    def ts_to_time(self, ts: int):
        return time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(ts))


if __name__ == '__main__':
    url = "wss://stream-testnet.bybit.com/realtime"
    subs = [
        "orderBook_200.100ms.BTCUSD",
        "trade.BTCUSD"
    ]

    handler = WebsocketHandler(url=url)
    handler.subscribe(subs)
