import pandas as pd
from sqlalchemy import create_engine

from _secrets import DB_URI


def save_symbols_for_hqm_score():
    engine = create_engine(DB_URI)

    my_symbols = pd.read_excel("resources/hqms_report_inputs.xlsx", index_col='symbol')

    query = "select symbol_id, symbol from symbol_view"
    symbols = pd.read_sql_query(query, con=engine, index_col='symbol')

    df2 = pd.merge(my_symbols, symbols, left_index=True, right_index=True)
    # save to database
    df2.to_sql('hqms_report_inputs', con=engine, index=False, if_exists='append')


if __name__ == '__main__':
    save_symbols_for_hqm_score()
