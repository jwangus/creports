from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from _secrets import DB_URI
from hqms_report_core import analysis
from iex_cloud_to_db import save_stats


def calc_hqm_matrix():
    # 1. prepare data
    engine = create_engine(DB_URI)
    # we need a list of symbol groups
    # the hqm scores are ranked only in the symbol group
    input = pd.read_sql_query(
        "select distinct symbol_group from hqms_report_inputs", con=engine)

    # 2. compute for each symbol group
    for symbol_group in input['symbol_group']:
        _run_analysis(engine, symbol_group)


def _run_analysis(engine, symbol_group):
    # load statistics
    sql = f"""\
        SELECT as_of, s.symbol_id, year1changepercent, month6changepercent, month3changepercent, month1changepercent, day5changepercent
        FROM  current_stats_view s 
        inner join hqms_report_inputs i on s.symbol_id = i.symbol_id
        where i.symbol_group='{symbol_group}'
        """
    df = pd.read_sql(sql, con=engine)
    # calculate the hqm scores
    # rename the columns
    df.columns = ['as_of', 'symbol_id', 'return_1y', 'return_6m', 'return_3m', 'return_1m', 'hqm_score']
    # add the new columns
    df['symbol_group'] = symbol_group
    df['dt_saved'] = datetime.utcnow()
    df['return_percentile_1y'] = pd.NA
    df['return_percentile_6m'] = pd.NA
    df['return_percentile_3m'] = pd.NA
    df['return_percentile_1m'] = pd.NA
    df['hqm_score'] = pd.NA
    hqms_df = analysis(df)
    # save to db
    hqms_df.to_sql('hqm_scores', con=engine, index=False, if_exists='append')


def save_symbols_for_hqm_score():
    engine = create_engine(DB_URI)

    my_symbols = pd.read_excel("resources/hqms_report_inputs.xlsx", index_col='symbol')

    query = "select symbol_id, symbol from symbol_view"
    symbols = pd.read_sql_query(query, con=engine, index_col='symbol')

    df2 = pd.merge(my_symbols, symbols, left_index=True, right_index=True)
    # save to database
    df2.to_sql('hqms_report_inputs', con=engine, index=False, if_exists='append')


def fetch_report_symbol_stats():
    engine = create_engine(DB_URI)
    query = "select distinct symbol from hqms_report_inputs_view"
    symbols = pd.read_sql_query(query, con=engine)

    save_stats(symbols['symbol'])


if __name__ == '__main__':
    # save_symbols_for_hqm_score()
    # fetch_report_symbol_stats()
    calc_hqm_matrix()
