import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

from _secrets import DB_NAME, DB_HOST, DB_PASS, DB_USER
from _secrets import IEX_API_TOKEN, IEX_API_BASE


def _create_insert_stats_sql(r):
    return f"""\
    INSERT INTO iex_stats (
        as_of,
        symbol,
        companyName,
        marketcap,
        week52high,
        week52low,
        week52highSplitAdjustOnly,
        week52lowSplitAdjustOnly,
        week52change,
        sharesOutstanding,
        avg10Volume,
        avg30Volume,
        day200MovingAvg,
        day50MovingAvg,
        employees,
        ttmEPS,
        ttmDividendRate,
        dividendYield,
        nextDividendDate,
        exDividendDate,
        nextEarningsDate,
        peRatio,
        beta,
        maxChangePercent,
        year5ChangePercent,
        year2ChangePercent,
        year1ChangePercent,
        ytdChangePercent,
        month6ChangePercent,
        month3ChangePercent,
        month1ChangePercent,
        day30ChangePercent,
        day5ChangePercent,
        dt_saved
    )
    VALUES (
        '{r['as_of']}',
        '{r['symbol']}',
        '{r['companyName']}',
        {r['marketcap']},
        {r['week52high']},
        {r['week52low']},
        {r['week52highSplitAdjustOnly']},
        {r['week52lowSplitAdjustOnly']},
        {r['week52change']},
        {r['sharesOutstanding']},
        {r['avg10Volume']},
        {r['avg30Volume']},
        {r['day200MovingAvg']},
        {r['day50MovingAvg']},
        {r['employees']},
        {r['ttmEPS']},
        {r['ttmDividendRate']},
        {r['dividendYield']},
        '{r['nextDividendDate']}',
        '{r['exDividendDate']}',
        '{r['nextEarningsDate']}',
        {r['peRatio']},
        {r['beta']},
        {r['maxChangePercent']},
        {r['year5ChangePercent']},
        {r['year2ChangePercent']},
        {r['year1ChangePercent']},
        {r['ytdChangePercent']},
        {r['month6ChangePercent']},
        {r['month3ChangePercent']},
        {r['month1ChangePercent']},
        {r['day30ChangePercent']},
        {r['day5ChangePercent']},
        '{r['dt_saved']}'
    )
                """


def save_iex_stats_to_db(symbols):
    api_url = f'{IEX_API_BASE}/stock/market/batch'

    api_params = {
        'token': IEX_API_TOKEN,
        'symbols': '',  # will fill in the loop
        'types': 'stats'
    }

    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    as_of_date = datetime.date.today()
    timestamp = datetime.datetime.utcnow()
    for symbol_chunk in _chunks(symbols, 100):
        api_params['symbols'] = ','.join(symbol_chunk)
        req_result = requests.get(api_url, params=api_params)
        print(f'api call status: {req_result.status_code}')
        data = req_result.json()

        for s in symbol_chunk:
            print(s)
            r = data[s]['stats']
            # change some data to format the insert stmt.
            for k in r:
                if k.endswith('Date'):
                    if r[k] == '0':
                        r[k] = '1970-01-01'
                r[k] = r[k] if r[k] else 'NULL'

            r['symbol'] = s
            r['as_of'] = str(as_of_date)
            r['dt_saved'] = str(timestamp)

            statement = _create_insert_stats_sql(r)
            print(statement)
            cursor.execute(statement)
    conn.commit()


def save_iex_symbols_to_db():
    api_url = f"{IEX_API_BASE}/ref-data/symbols"
    api_params = {
        'token': IEX_API_TOKEN,
    }

    req_result = requests.get(api_url, params=api_params)

    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for r in req_result.json():
        print(r)
        statement = f"""\
            INSERT INTO symbols (	symbol,exchange,exchangeSuffix,exchangeName,name,type,iexId,region,currency,isEnabled,figi, cik,lei	)
            VALUES(	'{r['symbol']}','{r['exchange']}','{r['exchangeSuffix']}','{r['exchangeName']}','{r['name']}','{r['type']}','{r['iexId']}','{r['region']}','{r['currency']}','{r['isEnabled']}','{r['figi']}','{r['cik']}','{r['lei']}'	)
        """
        cursor.execute(statement)
    conn.commit()


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def save_my_symbols_to_db():
    df = pd.read_csv('resources/my_symbols.csv')
    save_iex_stats_to_db(df['symbol'])


if __name__ == '__main__':
    # save_iex_symbols_to_db()
    save_my_symbols_to_db()
