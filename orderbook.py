import numpy as np
import json

"""
OrderBook

OrderBook takes a well formatted lob_event table as input and reconstructs 
every iteration of the order book at every timestamp.

For an example on how to use the OrderBook, see orderbook_example.py
"""
class OrderBook():
    TICKSIZE = 0.5
    ID_TICK = TICKSIZE*10e4

    """
    Initialises the orderbook to the initial state described in the snapshot.

    lob_events is a 2 dimensional numpy array containing order book data 
    as described by the medium article.
    """
    def __init__(self, lob_events):
        self.keys = {
            'event_no': 0,
            'order_id': 1,
            'side': 2,
            'price': 3,
            'size': 4,
            'lob_action': 5,
            'event_timestamp': 6,
            'order_type': 7,
            'order_cancelled': 8,
            'order_executed': 9,
            'execution_price': 10,
            'executed_size': 11,
            'agressor_side': 12,
            'trade_id': 13
        }

        self.orderbook = {}
        self.lob_events = lob_events
        self.best_bidask = None

        # assume that the first timestamp is for the snapshots
        self.curr_timestamp = lob_events[0][self.keys['event_timestamp']]
        self.curr_event_id = 1
        self.n_rows = len(lob_events)
        self.finished = False

        self._next_row()
        self._initialise_lob()

    def _next_row(self):
        if self.curr_event_id == self.n_rows:
            self.finished = True
        else:
            self.row = self.lob_events[self.curr_event_id-1]
            self.curr_event_id += 1

    def _initialise_lob(self):
        if self.curr_event_id == self.n_rows:
            self.finished = True
            return

        while not self.finished and self.row[self.keys['event_timestamp']] == self.curr_timestamp:
            if self.row[self.keys['lob_action']] == 2:
                self.orderbook[self.row[self.keys['order_id']]] = {
                    'side': self.row[self.keys['side']],
                    'price': self.row[self.keys['price']],
                    'size': self.row[self.keys['size']]
                }
            else:
                raise Exception("Unexpected LOB Action while parsing Snapshots.")
            self._next_row()
        self.curr_timestamp = self.row[self.keys['event_timestamp']]
    
    def get_state(self):
        return self.orderbook
    
    def update_order_book(self):
        self.best_bidask = None
        while not self.finished and self.row[self.keys['event_timestamp']] == self.curr_timestamp:
            if self.row[self.keys['lob_action']] in (2, 4):
                self.orderbook[self.row[self.keys['order_id']]] = {
                    'side': self.row[self.keys['side']],
                    'price': self.row[self.keys['price']],
                    'size': self.row[self.keys['size']]
                }
            elif self.row[self.keys['lob_action']] == 3:
                del self.orderbook[self.row[self.keys['order_id']]]
            self._next_row()
        self.curr_timestamp = self.row[self.keys['event_timestamp']]
        return self.orderbook
    
    def get_best_bid_price(self):
        if self.best_bidask == None:
            self._find_price_index()
        return float(self.orderbook[self.best_bidask[0]]['price'])

    def get_best_ask_price(self):
        if self.best_bidask == None:
            self._find_price_index()
        return float(self.orderbook[self.best_bidask[1]]['price'])

    def get_best_bid_size(self):
        if self.best_bidask == None:
            self._find_price_index()
        return int(self.orderbook[self.best_bidask[0]]['size'])

    def get_best_ask_size(self):
        if self.best_bidask == None:
            self._find_price_index()
        return int(self.orderbook[self.best_bidask[1]]['size'])

    def _find_price_index(self):
        prev_key = None
        for key in list(sorted(self.orderbook.keys())):
            if self.orderbook[key]['side'] != 1:
                self.best_bidask = (prev_key, key)
                return
            else:
                prev_key = key
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if not self.finished:
            return self.update_order_book()
        else:
            raise StopIteration()