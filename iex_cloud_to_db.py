from datetime import datetime, date

import pandas as pd
import requests
from sqlalchemy import create_engine

from _secrets import DB_URI
from _secrets import IEX_API_TOKEN, IEX_API_BASE
from creports_utils import chunks


def save_all_symbols():
    req_results = _request_symbols()
    if req_results.status_code == 200:
        _save_to_symbol(req_results.json())
    else:
        print(f'error requesting stats.  status_code = {req_results.status_code}')

def _save_to_symbol(symbols):
    df = pd.DataFrame(symbols)
    # 1. rename the columns to lower case
    df.columns = [c.lower() for c in df.columns]

    # 2. save to db
    engine = create_engine(DB_URI)
    df.to_sql('symbol', con=engine, if_exists='append', index=False)


def _request_symbols():
    api_url = f"{IEX_API_BASE}/ref-data/symbols"
    api_params = {
        'token': IEX_API_TOKEN,
    }
    return requests.get(api_url, params=api_params)


def save_stats(symbols):
    for chunk in chunks(symbols, 100):
        # IEX batch only allows up to 100 symbols at a time
        req_results = _request_stats(chunk)
        if req_results.status_code == 200:
            # successful requst
            as_of = date.today()
            dt_saved = datetime.utcnow()
            _save_to_iex_stats(req_results.json(), as_of, dt_saved)
        else:
            print(f'error requesting stats.  status_code = {req_results.status_code}')


def _request_stats(symbols):
    api_url = f'{IEX_API_BASE}/stock/market/batch'
    api_params = {
        'token': IEX_API_TOKEN,
        'symbols': ','.join(symbols),
        'types': 'stats,price',
    }
    return requests.get(api_url, params=api_params)


def _save_to_iex_stats(req_results, as_of, dt_saved):
    engine = create_engine(DB_URI)
    # read symbol -> symbol_id mapping
    symbols = pd.read_sql_table('symbols', con=engine, index_col='symbol', columns=['id'], )

    # 1. create an empty datafrom by selecting all columns from iex_stats table
    df = pd.read_sql_query("select * from iex_stats where false", con=engine)
    # 2. loop through the results and create one row per symbol
    for symbol in req_results:
        row = req_results[symbol]['stats']
        # create a new dict with lower key
        dt_row = {'as_of' : as_of, 'dt_saved': dt_saved}
        for r_k in row:
            dt_row[r_k.lower()] = row[r_k]

        dt_row['symbol_id'] = symbols.loc[symbol]['id']
        df = df.append(dt_row, ignore_index=True)

    # 3. clean up the dataframe before saving to iex_stats table
    del df['id']
    del df['float']
    del df['nextdividenddate']

    df.to_sql('iex_stats', con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    # save_stats(['AAPL'])
    save_all_symbols()
