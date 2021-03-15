
import secrets
import requests
import pandas as pd
import yfinance as yf
import numpy as np
import datetime


def save_trans(wallet):
    payload = {"module": "account", "action": "txlist", "address": wallet, "startblock": 0000000, "endblock": 99999999,
               "sort": "asc", "apikey": secrets.ETHERSCAN_API_KEY}
    # startblock 10000000 is used, older wallets crash because etherscan used to save their data differently

    r = requests.get("https://api.etherscan.io/api", params=payload)

    df = pd.DataFrame(r.json()["result"])

    tx_file_name = f"resources/tx_{wallet}.csv"
    df.to_csv(tx_file_name)

    return tx_file_name


def save_eth_price():
    df_yahoo = yf.download("ETH-USD", start="2020-12-01", end=datetime.date.today(), progress=False)
    px_file_name = "resources/ETH-USD.csv"
    df_yahoo.to_csv(px_file_name)

    return px_file_name


def load_data(tx_file_name, pool_addr):
    tx = pd.read_csv(tx_file_name,
                     usecols=['timeStamp', 'value', 'gasPrice', 'gasUsed', 'from'],
                     converters={'value': int})
    flt = (tx['from'] == pool_addr)
    return (tx.loc[flt],
            pd.read_csv("resources/ETH-USD.csv", usecols=['Date', 'Adj Close']))


def prepare_calc_df(tx, px, step_seconds=600):
    # calculate average
    # t0,  t1 - t0             v1
    # t1,  t2 - t1 = timeSpan  v2
    # t2,  t3 - t2 = timeSpan  v3
    # ...
    # tn-1,  tn - tn-1, vn
    tx['timeSpan'] = (tx['timeStamp'].shift(-1)-tx['timeStamp'])/step_seconds
    tx['avg'] = pd.to_numeric(tx['value']).shift(-1) / tx['timeSpan']
    # deal with NaN in the last row
    tx.loc[tx.iloc[-1].name, 'avg'] = tx.loc[tx.iloc[-2].name, 'avg']

    # px convert date to timeStamp
    px['timeStamp'] = pd.to_datetime(px['Date']).apply(datetime.datetime.timestamp).apply(int)

    # construct our evenly spaced dataframe and join with transaction and px
    calc_df = pd.DataFrame({'timeStamp': np.arange(tx['timeStamp'].min(), tx['timeStamp'].max()+10*3600, step_seconds)})
    calc_df = pd.merge_asof(calc_df, tx, on='timeStamp')
    calc_df = pd.merge_asof(calc_df, px, on='timeStamp')

    calc_df['avg_dollar'] = calc_df['avg'] * calc_df['Adj Close']
    return calc_df


def compute_improved(calc_df, start_timestamp, end_timestamp, elec_cost=0, other_cost=0, step_seconds=600):
    # get the region for integration or summation
    flt = ((calc_df['timeStamp'] >= int(start_timestamp)) & (calc_df['timeStamp'] < int(end_timestamp)))
    y_avg = calc_df.loc[flt, 'avg']
    y_avg_dollar = calc_df.loc[flt, 'avg_dollar']
    y_avg_px = calc_df.loc[flt, 'Adj Close']

    return {'sum_mined': y_avg.sum()/1e18, 'sum_mined_dollar': y_avg_dollar.sum()/1e18, 'avg_price': y_avg_px.mean()}


if __name__ == '__main__':
    # test code
    trans = save_trans(secrets.MY_WALLET)
    price = save_eth_price()

    calc_dataframe = prepare_calc_df(trans, price)
