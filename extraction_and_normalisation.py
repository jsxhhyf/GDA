import json
import pprint
import sys
import threading
import time

import numpy as np
import pandas as pd
import websocket
from loguru import logger

INTERVAL_BEFORE = 500
INTERVAL_AFTER = 200  # 200 ms

logger.remove()
std_logger = logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{"
    "name}</cyan>:<cyan>{function:<20}</cyan>:<cyan>{line:<3}</cyan> | <cyan>{"
    "thread.name}</cyan> - <level>{message}</level>",
)
# file_logger = logger.add(
#     "./output/loguru_log.log",
#     format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{"
#     "name}</cyan>:<cyan>{function:<20}</cyan>:<cyan>{line:<3}</cyan> | <cyan>{"
#     "thread.name}</cyan> - <level>{message}</level>",
#     level="INFO" if PRICE_ON else "DEBUG",
# )


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
            on_error=lambda ws, err: self._on_error(err),
        )
        self.util = Util()
        self.lob = Handler()

        self.msg_queue = []
        self.lock = threading.Lock()

        self.connector = threading.Thread(
            name="connector",
            target=lambda: self.ws.run_forever(
                ping_interval=30,
                ping_timeout=10,
                # ping_payload='{"op":"ping"}'
            ),
        )

        self.connector.daemon = True
        self.connector.start()

        self.consumer = threading.Thread(
            name="consumer", target=lambda: self.fetch(self.msg_queue)
        )
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
        logger.info(f"WebSocket opened.")

    def _on_close(self):
        """
        Log WS close.
        """
        logger.info(f"WebSocket closed.")

    def fetch(self, msg_list: list):
        """
        Fetch msg from the queue, invoke on_XXX methods depending on the msg type
        Args:
            msg_list (list): the msg_queue storing msg sent from the server

        Returns:

        """
        while True:
            if msg_list:
                self.lock.acquire()
                msg_json = json.loads(msg_list.pop(0))
                # logger.debug('msg consumed! msg_list length: {}', len(msg_list))
                self.lock.release()
            else:
                time.sleep(0.5)
                continue

            # if 'success' in msg_json:
            #     logger.info(f"Subscription of {msg_json.get('request').get('args')} is succeeded")
            #     continue

            if "topic" in msg_json and "orderBook" in msg_json.get("topic"):

                # align the ts to trade_record's ts
                msg_json["timestamp_e6"] = int(msg_json["timestamp_e6"] // 1e3)
                if msg_json.get("type") == "snapshot":
                    self.lob.on_snapshot(msg_json)
                elif msg_json.get("type") == "delta":
                    self.lob.on_delta(msg_json)
                    # self.lob.save()
            elif "topic" in msg_json and "trade" in msg_json.get("topic"):
                self.lob.on_trade(msg_json)


class Handler:
    """
    Limited Order Book
    """

    EVENT_NO = 1

    def __init__(self):
        self.util = Util()

        self.lob_event = None
        self.delta_cache = []
        self.trade_cache = []
        self.snapshot = {}
        self.market_order_book = None

    def on_snapshot(self, msg_json: "json"):
        """
        Process snapshot message.
        Args:
            msg_json (json): json message extracted from msg_queue

        Returns:
            None

        """

        # self.debug_on_receive("snapshot", msg_json)
        self.reset_lob_event()

        _snap_array = []

        data = msg_json["data"]
        ts = msg_json["timestamp_e6"]

        for record in data:
            temp = [
                self.get_event_no(),
                record["id"],
                Util.d_buy_sell[record["side"]],
                record["price"],
                record["size"],
                np.nan,
                ts,
                Util.d_order_type["limit_orders"],
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ]
            _snap_array.append(temp)

            # create a dict of price, side, and size
            self.snapshot[record["price"]] = [record["size"], record["side"]]

        self.lob_event = np.array(_snap_array)

        # self.display_snapshot()

    def on_delta(self, delta: 'json'):
        """
        Process delta message.
        Cache the delta message in delta_cache for further processing.

        Args:
            delta (json): json message extracted from msg_queue

        Returns:
            None

        """
        # self.debug_on_receive("delta", delta)

        # Sync snapshot
        for action in Util.l_actions:
            for delta_record in delta['data'][action]:
                self.sync_snapshot(delta_record, action=action)

        # Cache newly arrived delta
        self.delta_cache.append(delta)

        # logger.debug('ready_event_list with len: \n{}', ready_event_list)

    def on_trade(self, trade: "json"):
        """
        Process trade message.
        1. cache it in trade_cache
        2. iterate every delta msg in delta_cache
        3. compare the ts of trade_record and delta_record
        4. generate lob_event
        5. generate market_order_book

        Args:
            trade (json): the trade msg received

        Returns:

        """

        self.debug_on_receive("trade", trade)

        for trade_record in trade['data']:
            self.trade_cache.append(trade_record)

        trades_temp_list = []
        ready_event_list = []

        for trade_record in self.trade_cache:

            logger.debug('checking trade {}', trade_record)

            trade_ts = trade_record['trade_time_ms']

            # for market order book
            temp_market_order = [
                int(float(trade_record["price"]) * 1e5),
                trade_record["price"],
                trade_record["trade_id"],
                trade_record["trade_time_ms"],
                Util.d_buy_sell[trade_record["side"]],
                trade_record["size"],
            ]
            trades_temp_list.append(temp_market_order)

            # start iterating delta_cache
            for delta in self.delta_cache[:]:

                delta_data = delta['data']
                delta_ts = delta['timestamp_e6']
                # if trade is much earlier than delta
                # check the next trade
                if trade_ts - delta_ts < -INTERVAL_BEFORE:
                    logger.debug('### ANCIENT TRADE ### @ {}', trade_ts % 1e8)
                    # FIXME
                    self.trade_cache.remove(trade_record)
                    break
                # if trade is much later than delta
                # check the next delta in delta_cache
                elif trade_ts - delta_ts > INTERVAL_AFTER:
                    ready_event_list.extend(
                        self.generate_event_from_delta_data(delta_data, delta_ts)
                    )
                    # remove the delta
                    self.delta_cache.remove(delta)
                # if trade is approximately at the same time of delta
                else:
                    for action in ["delete", "update", "insert"]:
                        for delta_record in delta_data[action]:

                            logger.debug(
                                'comparing trade_ts: {}, delta_ts: {}', trade_ts, delta_ts
                            )

                            if float(trade_record['price']) == float(delta_record['price']):

                                ready_event_list.extend(
                                    self.on_match(
                                        delta_record, trade_record, action=action
                                    )
                                )
                                # every trade_record could possibly match to one delta_record only
                                self.trade_cache.remove(trade_record)
                                break
                        else:
                            continue
                        break
                    else:
                        continue
                    break

        # merge ready_event_list to lob_event
        if self.lob_event is not None:
            if len(self.lob_event.shape) != len(np.array(ready_event_list).shape):
                logger.debug('{}, {}', self.lob_event.shape, np.array(ready_event_list).shape)
            else:
                self.lob_event = np.concatenate(
                    [self.lob_event, np.array(ready_event_list)], axis=0
                )
        else:
            self.lob_event = np.array(ready_event_list)

        # add to market_order_book
        if self.market_order_book is not None:
            self.market_order_book = np.append(
                self.market_order_book, np.array(trades_temp_list), axis=0
            )
        else:
            self.market_order_book = np.array(trades_temp_list)

        self.save()

    def generate_event_from_delta_data(self, delta_data: 'json', ts: int) -> list:
        """
        Generate lob event from a series of delta_record.

        Args:
            delta_data (json): the 'data' section of a delta msg
            ts (int): the timestamp of the delta msg

        Returns:
            return a list of lists containing all the lob events

        """

        ret_list = []

        delta_ts = ts

        for action in Util.l_actions:
            for delta_record in delta_data[action]:
                ret_list.append(
                    self.generate_event_from_delta_record(
                        delta_record, action, delta_ts
                    )
                )
        logger.debug('returning {}', ret_list)
        return ret_list

    def generate_event_from_delta_record(
        self, delta_record: 'json', action: str, ts: int
    ) -> list:
        """
        Generate lob events from a singe delta record

        Args:
            delta_record (json): a delta record belongs to one of the type [
            'delete', 'update', 'insert']
            action (str): ['delete', 'update', 'insert']
            ts (int): the timestamp of the enclosing delta

        Returns:
            temp (list): a list of designed attributes
        """

        temp = [
            self.get_event_no(),
            delta_record["id"],
            Util.d_buy_sell[delta_record["side"]],
            delta_record["price"],
            delta_record["size"],
            Util.d_action[action],
            ts,
            1,
            # trade will only cause size drop down
            0 if action == "insert" else 1,
            0,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ]
        return temp

    def on_match(self, delta_record: 'json', trade_record: 'json', action: str) -> list:
        """
        Generate a market event and a limit event when a trade record matches a delta record

        Args:
            delta_record (json): one of update, delete or insert records
            trade_record (json): a single trade record within a trade message
            action (str): action type for the delta_record

        Returns:
            [temp_market, temp_limit]: the list containing the market event generated by trade_record,
            and the corresponding limit event

        """

        if trade_record["side"] != delta_record["side"]:

            logger.debug('match found! {}', trade_record['price'])

            if action == "delete":
                delta_record["size"] -= trade_record["size"]
            elif action == "insert":
                delta_record["size"] += trade_record["size"]

            # generated market order
            temp_market = [
                self.get_event_no(),  # event_no
                delta_record["id"],  # order_id
                Util.d_buy_sell[trade_record["side"]],  # side
                delta_record["price"],  # price
                trade_record["size"],  # size
                1,  # lob_action
                trade_record["trade_time_ms"],  # event_ts
                Util.d_order_type["market_orders"],
                # order_type
                0,  # order_cancelled
                1,  # order_executed
                delta_record["price"],  # execution_price
                trade_record["size"],  # execution_size
                Util.d_buy_sell[trade_record["side"]],  # agressor_side
                trade_record["trade_id"],  # trade_id
            ]

            # corresponding limit order
            temp_limit = [
                self.get_event_no(),  # event_no
                delta_record["id"],  # order_id
                3 - Util.d_buy_sell[trade_record["side"]],  # side
                delta_record["price"],  # price
                trade_record["size"],  # size
                1,  # lob_action
                trade_record["trade_time_ms"],  # event_ts
                Util.d_order_type["limit_orders"],  # order_type
                0,  # order_cancelled
                1,  # order_executed
                delta_record["price"],  # execution_price
                trade_record["size"],  # execution_size
                Util.d_buy_sell[trade_record["side"]],  # agressor_side
                trade_record["trade_id"],  # trade_id
            ]

            return [temp_market, temp_limit]
        else:

            logger.warning("SAME SIDE DETECTED IN {} DELTA!", action.upper())
            logger.debug("trade_record: {}", trade_record)
            logger.debug("delta: {}", delta_record)

    def sync_snapshot(self, delta_record: 'json', action: str):
        """
        Merge a delta record into the snapshot.
        By calling this function the snapshot is maintained up-to-date

        Args:
            delta_record (json): a single delta record
            action (str): ['delete', 'update', 'insert']

        Returns:

        """

        if action == "delete":
            # Fill in the size of delete delta
            delta_record['size'] = self.snapshot[delta_record['price']][0]
            # logger.debug(
            #     'delete record {} size has been filled: {}',
            #     delta_record['price'],
            #     self.snapshot[delta_record['price']][0],
            # )
            self.snapshot.pop(delta_record["price"])
            # logger.debug("Adding to SNAPSHOT: DELETE at price {}", delta_record["price"])
        elif action == "update":
            self.snapshot[delta_record["price"]][0] = delta_record["size"]
            # logger.debug("Adding to SNAPSHOT: UPDATE at price {}", delta_record["price"])
        elif action == "insert":
            self.snapshot[delta_record["price"]] = [
                delta_record["size"],
                delta_record["side"],
            ]
            # logger.debug("Adding to SNAPSHOT: INSERT at price {}", delta_record['price'])

    def clear_delta_cache(self):
        logger.debug("clearing trade_cache.")
        self.delta_cache.clear()

    def save(self):
        logger.debug("Start writing to csv files...")
        if self.lob_event is not None:
            np.savetxt(
                "./output/lob_events.csv", self.lob_event, delimiter=",", fmt="%s"
            )
        if self.market_order_book is not None:
            np.savetxt(
                "./output/market_order_book.csv",
                self.market_order_book,
                delimiter=",",
                fmt="%s",
            )

    def reset_lob_event(self):
        self.lob_event = None

    def get_timestamp(self, message_type: str, j: "json") -> list:

        ts_list = []

        if message_type == "trade":
            for data in j["data"]:
                ts_list.append(int(data["trade_time_ms"] % 1e8))

        elif message_type == "delta" or message_type == "snapshot":
            ts_list.append(int(j["timestamp_e6"] % 1e8))

        return ts_list

    def debug_on_receive(self, message_type: str, j: "json"):

        if message_type == "delta":
            for action in ["delete", "update", "insert"]:
                if len(j["data"][action]) != 0:
                    logger.debug(
                        "{:<9s} packet received! price: {} @ ts: {}",
                        action.upper(),
                        [d["price"] for d in j["data"][action]],
                        self.get_timestamp(message_type, j),
                    )

        else:
            logger.debug(
                "{:<9s} packet received! price: {} @ ts: {}",
                message_type.upper(),
                [d["price"] for d in j["data"]],
                self.get_timestamp(message_type, j),
            )

    def display_snapshot(self):

        # sort lob by price
        sorted_lob = dict(sorted(self.snapshot.items(), key=lambda d: d[0]))

        lob_df = pd.DataFrame({"price": sorted_lob.keys()})
        lob_df["size"] = [item[0] for item in sorted_lob.values()]
        lob_df["side"] = [item[1] for item in sorted_lob.values()]
        # print(lob_df)

        # lob_df.to_csv("./output/snapshot.csv")

    @staticmethod
    def get_event_no():
        ret = Handler.EVENT_NO
        Handler.EVENT_NO += 1
        return ret


class Util:
    d_buy_sell = {
        "Unkonwn": 0,
        "Buy": 1,
        "Sell": 2,
    }
    d_action = {"unknown": 0, "skip": 1, "insert": 2, "delete": 3, "update": 4}
    d_order_type = {"unknown": 0, "limit_orders": 1, "market_orders": 2}
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
        "trade_id": 13,
    }
    l_actions = ["delete", "update", "insert"]

    def __init__(self):
        pass

    def ts_to_time(self, ts: int):
        return time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(ts))


if __name__ == "__main__":
    url = "wss://stream-testnet.bybit.com/realtime"
    subs = ["orderBook_200.100ms.BTCUSD", "trade.BTCUSD"]

    handler = WebsocketHandler(url=url)
    handler.subscribe(subs)
