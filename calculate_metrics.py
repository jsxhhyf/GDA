from pandas._libs.tslibs import timestamps
from orderbook import OrderBook

import timeit
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS
import matplotlib.pyplot as plt


def plot_ofi_and_ols_summary(df, dt):
    p = df.plot(kind='scatter', grid=True, 
            x='OFI', y='Mid-Price Change', 
            title = 'BTCUSD '+ dt + ' OFI',
            alpha=0.5, figsize=(12,10))
    ofi_ = sm.add_constant(df['OFI'])
    ols = OLS(df['Mid-Price Change'].astype(float).div(0.5), ofi_.astype(float)).fit()
    p.get_figure().savefig('ofi_ols.pdf')
    print(ols.summary2())


def plot_tfi_and_ols_summary(tf, dmid, dt):
    p = pd.concat([tf,dmid], axis = 1).plot(kind='scatter', grid=True, 
                                        x='TFI', y='Mid-Price Change', 
                                        title = 'XBTUSD '+ dt + ' TFI', 
                                        alpha=0.5, figsize=(12,10))
    tf_ = sm.add_constant(tf)
    ols = OLS(dmid, tf_).fit()
    p.get_figure().savefig('tfi_ols.pdf')
    print(ols.summary2())


def calculate_ofi(lob_events):
    t1 = timeit.default_timer()
    book = OrderBook(lob_events)
    t2 = timeit.default_timer()
    print(f"Order Book Init Time: {(t2-t1)*10e3}ms")

    ofi_df = pd.DataFrame(columns = [
        "OFI",
        "Mid-Price Change",
        "Micro-Price Change"
    ])

    timestamps_used = set()
    dmp_hashmap = {}

    prev_bidprice = None
    prev_bidsize = None
    prev_askprice = None
    prev_asksize = None
    prev_mp = None
    prev_micro_p = None

    # Performance testing chunk of code
    total_lob_update_time = 0
    total_calculation_time = 0 # Includes OFI, Mid-Point price, Mid-Point Price Change, Microprice, Microprice Change
    loops = 0

    # Resetting t1 for first iteration of loop
    t1 = timeit.default_timer()
    for state in book:
        t2 = timeit.default_timer()
        total_lob_update_time += t2 - t1
        book_ts = book.curr_timestamp
        timestamps_used.add(book_ts)

        t1 = timeit.default_timer()
        ofi = None
        mp_change = None
        micro_p_change = None

        bidprice = book.get_best_bid_price() 
        bidsize = book.get_best_bid_size()
        askprice = book.get_best_ask_price()
        asksize = book.get_best_ask_size()

        # Calculating midpoint price
        mp = (bidprice + askprice)/2

        # Calculating microprice
        micro_p = bidsize/(bidsize + asksize)*bidprice + asksize/(bidsize+ asksize)*askprice
    
        # Calculating midpoint price change
        if prev_mp != None:
            mp_change = mp - prev_mp

        # Calculating microprice change
        if prev_mp != None:
            micro_p_change = micro_p - prev_micro_p

        if prev_bidprice != None:
            ofi = 0
            if bidprice >= prev_bidprice:
                ofi += bidsize

            if bidprice <= prev_bidprice:
                ofi -= prev_bidsize

            if askprice >= prev_askprice:
                ofi += prev_asksize

            if askprice <= prev_askprice:
                ofi -= asksize

        prev_bidprice = bidprice
        prev_bidsize = bidsize
        prev_askprice = askprice
        prev_asksize = asksize
        prev_mp = mp
        prev_micro_p = micro_p
        
        if ofi != None and mp_change != None:
            dmp_hashmap[book_ts] = mp_change
            ofi_df = ofi_df.append(
                {
                    'OFI': ofi, 
                    'Mid-Price Change': mp_change, 
                    'Micro-Price Change': micro_p_change
                }, 
                ignore_index=True
            )
        t2 = timeit.default_timer()
        total_calculation_time += t2 - t1
        
        loops += 1
        t1 = timeit.default_timer()

    print(f"Average Order Book Update Time: {round((total_lob_update_time)*10e3, 3)/loops}ms")
    print(f"Average Metric Calculation Time (Not Including TFI): {round((total_calculation_time)*10e3, 3)/loops}ms")
    timestamps_used = sorted(timestamps_used)
    return ofi_df, timestamps_used, dmp_hashmap


def calculate_tfi(trades, timestamps_used, dmp_hashmap):
    tfi_df = pd.DataFrame(columns = [
        "TFI",
        "Mid-Price Change"
    ])

    trades_df = pd.DataFrame(data = trades)
    trades_df = trades_df.set_axis(["order_id","price","trade_id","timestamp","side","size"], axis=1, inplace=False)

    trades_df['sgn_size'] = np.where(trades_df['side'] == 1, trades_df['size'], -trades_df['size'])
    trades_df['Mid-Price Change'] = 0.0
    trades_df["date"] = pd.to_datetime(trades_df["timestamp"], unit="ms")

    i = 0
    for ts in timestamps_used:
        while ((trades_df.iloc[i]['timestamp'] > ts - 10e2 and trades_df.iloc[i]['timestamp'] <= ts)
               and i < len(timestamps_used)):
            trades_df.iloc[i]['Mid-Price Change'] = dmp_hashmap[ts]

    dmid = trades_df[['date', 'Mid-Price Change']].resample("10L", on="date").sum().fillna(0.0).reset_index()['Mid-Price Change']
    tf = trades_df[['date', 'sgn_size']].resample("10L", on="date").sum().fillna(0.0).reset_index()['sgn_size']
    assert dmid.count() == tf.count()
    tf.name = 'TFI'
    dmid.name = 'Mid-Price Change'

    return tf, dmid




def main():
    lob_events = np.genfromtxt('lob_data.csv', delimiter=',', dtype=None, skip_header=1, encoding=None)
    trades = np.genfromtxt('market_order_book.csv', delimiter=',', dtype=None, skip_header=1, encoding=None)

    ofi_df, timestamps_used, dmp_hashmap = calculate_ofi(lob_events)

    # timestamps and dmp_hashmap needed to match mid-price change to market order events
    tf, dmid = calculate_tfi(trades, timestamps_used, dmp_hashmap)

    plot_ofi_and_ols_summary(ofi_df, "100L")
    plot_tfi_and_ols_summary(tf, dmid, "10L")

if __name__ == "__main__":
    main()