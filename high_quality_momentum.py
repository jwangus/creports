import datetime
from statistics import mean
from _secrets import IEX_API_TOKEN

import pandas as pd
import requests
from scipy import stats


def get_data(symbols):
    symbol_str = ','.join(symbols)
    api_url = f'https://sandbox.iexapis.com/stable/stock/market/batch'

    my_cols = ['symbol', 'price',
               '1y_return', '1y_return_percentile',
               '6m_return', '6m_return_percentile',
               '3m_return', '3m_return_percentile',
               '1m_return', '1m_return_percentile',
               'hqm_score'
               ]
    f = pd.DataFrame(columns=my_cols)

    for symbol_chunk in chunks(symbols, 100):
        api_params = {
            'token': IEX_API_TOKEN,
            'symbols': ','.join(symbol_chunk),
            'types': 'stats,price'
        }
        req_result = requests.get(api_url, params=api_params)
        print(f'api call status: {req_result.status_code}')
        data = req_result.json()

        for s in symbol_chunk:
            f = f.append(
                pd.Series(
                    [
                        s, data[s]['price'],
                        data[s]['stats']['year1ChangePercent'], 'N/A',
                        data[s]['stats']['month6ChangePercent'], 'N/A',
                        data[s]['stats']['month3ChangePercent'], 'N/A',
                        data[s]['stats']['month1ChangePercent'], 'N/A',
                        'N/A'
                    ], index=my_cols),
                ignore_index=True
            )
    return f


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def analysis(hqm_df):
    time_periods = ['1y', '6m', '3m', '1m']
    for row in hqm_df.index:
        for tp in time_periods:
            chg_col = f'{tp}_return'
            percentile_col = f'{tp}_return_percentile'
            hqm_df.loc[row, percentile_col] = \
                stats.percentileofscore(hqm_df[chg_col], hqm_df.loc[row, chg_col])

    # compute hqm score, which is the average of the 1y, 6m, 3m, 1m percentiles
    for row in hqm_df.index:
        momentum_percentiles = []
        for tp in time_periods:
            momentum_percentiles.append(hqm_df.loc[row, f'{tp}_return_percentile'])
        hqm_df.loc[row, 'hqm_score'] = mean(momentum_percentiles)

    hqm_df.sort_values('hqm_score', ascending=False, inplace=True)
    hqm_df = hqm_df[:50]
    hqm_df.reset_index(inplace=True)
    return hqm_df


if __name__ == '__main__':
    etfs = pd.read_csv('resources/eft_sector.csv')

    df = get_data(etfs['symbol'])

    hqm_dataframe = analysis(df)
    final_df = pd.merge(hqm_dataframe, etfs, on='symbol')

    print(final_df)

    final_df.to_csv(f'resources/hqm_{datetime.date.today()}.csv')
