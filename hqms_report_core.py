import datetime
from statistics import mean

import pandas as pd
from scipy import stats


def analysis(hqm_df):
    time_periods = ['1y', '6m', '3m', '1m']
    for row in hqm_df.index:
        for tp in time_periods:
            chg_col = f'return_{tp}'
            percentile_col = f'return_percentile_{tp}'
            hqm_df.loc[row, percentile_col] = \
                stats.percentileofscore(hqm_df[chg_col], hqm_df.loc[row, chg_col])

    # compute hqm score, which is the average of the 1y, 6m, 3m, 1m percentiles
    for row in hqm_df.index:
        momentum_percentiles = []
        for tp in time_periods:
            momentum_percentiles.append(hqm_df.loc[row, f'return_percentile_{tp}'])
        hqm_df.loc[row, 'hqm_score'] = mean(momentum_percentiles)

    hqm_df.sort_values('hqm_score', ascending=False, inplace=True)
    return hqm_df


if __name__ == '__main__':
    pass
