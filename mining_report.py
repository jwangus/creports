import datetime
import math
import secrets
import pandas as pd

import creport_email
import mining_report_core as core


def report_html(r):
    html_head = """\
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    table, th, td {
      border: 1px solid black;
      border-collapse: collapse;
    }
    th, td {
      padding: 5px;
      text-align: left;
    }
    </style>
    </head>
    """
    html_body = f"""\
    <body>

    <h2>{r['num_days']} Day Mining Report</h2>

    <table style="width:100%">
      <caption></caption>
      <tr>
        <td>ETH Address</td>
        <td><a href="https://etherscan.io/address/{r['customer_wallet']}">{str.upper(r['customer_wallet'])}</a></td>
      </tr>
      <tr>
        <td>Mining Pool</td>
        <td><a href="https://etherscan.io/address/{r['customer_miningpool']}">{str.upper(r['customer_miningpool'])}</a></td>
      </tr>
      <tr>
        <td>Date Ending</td>
        <td>{r['end_date']:%Y-%m-%d} ({r['end_date']:%A})</td>
      </tr>
      <tr>
        <td>Number of days</td>
        <td>{r['num_days']}</td>
      </tr>
      <tr>
        <td>Average ETH Price (% +up/-down)</td>
        <td>${r['avg_price']:.2f} ({'+' if r['avg_price_chg'] > 0 else ''}{r['avg_price_chg']:.0f}%)</td>
      </tr>
      <tr>
        <td>Number of Rigs</td>
        <td>Unknown</td>
      </tr>
    </table>
    <p></p>
    <table style="width:100%">
      <caption>Summary</caption>
      <tr>
        <th></th>
        <th>ETH</th>
        <th>USD</th>
      </tr>
      <tr>
        <td>Mined Total</td>
        <td>{r['sum_mined']:.3f} ({'+' if r['sum_mined_chg'] > 0 else ''}{r['sum_mined_chg']:.0f}%)</td>
        <td>{r['sum_mined_dollar']:.2f} ({'+' if r['sum_mined_dollar_chg'] > 0 else ''}{r['sum_mined_dollar_chg']:.0f}%)</td>
      </tr>
      <tr>
        <td>Electricity Cost</td>
        <td>{r['electricity_cost']:.3f}</td>
        <td>{r['electricity_cost_dollar']:.2f}</td>
      </tr>
      <tr>
        <td>Other Cost</td>
        <td>{r['other_cost']:.3f}</td>
        <td>{r['other_cost_dollar']:.2f}</td>
      </tr>
      <tr>
        <td>Net Income</td>      
        <td>{r['net_income']:.3f}</td>
        <td>{r['net_income_dollar']:.2f}</td>
      </tr>

    </table>

    </body>
    </html>
    """
    return html_head + html_body


def chg_percent(v1, v2):
    return (v1 - v2) / v2 * 100 if v2 != 0.0 else math.nan


# reportPeriod can be 1w or 1m
def customer_report(wallet, pool_wallet, to_email, period_end_date, period_days=7, download_price=True):
    # download wallet transaction data from Etherscan
    tx_name = core.save_trans(wallet)
    if download_price:
        core.save_eth_price()

    # start calculation
    # 1. transaction history is indexed by time stamps that are not evenly spaced.  The prepare calc function
    #    will reindex the transaction history to have a evenly spaced time interval of e.g. 10min.
    #    So in later step, we can calculate the mining yields of any time interval by summing the yields of the smaller
    #    time period.
    tx, px = core.load_data(tx_name, pool_wallet)
    calc_df = core.prepare_calc_df(tx, px)

    # 2. Calculate last time period's yield.
    period_seconds = period_days * 24 * 3600
    end_ts = datetime.datetime(period_end_date.year, period_end_date.month, period_end_date.day).timestamp()
    r_period = core.compute_improved(calc_df, end_ts - period_seconds, end_ts)
    print(r_period)

    # 3. Calculate last last time period's yield so that we know the changes.
    end_ts -= period_seconds
    r_pre_period = core.compute_improved(calc_df, end_ts - period_seconds, end_ts)
    print(r_pre_period)

    params = {"sum_mined": r_period['sum_mined'],
              "sum_mined_chg": chg_percent(r_period['sum_mined'], r_pre_period['sum_mined']),
              "sum_mined_dollar": r_period['sum_mined_dollar'],
              "sum_mined_dollar_chg": chg_percent(r_period['sum_mined_dollar'], r_pre_period['sum_mined_dollar']),
              "avg_price": r_period['avg_price'],
              "avg_price_chg": chg_percent(r_period['avg_price'], r_pre_period['avg_price']),
              "tx": 0,
              "tx_chg": 0,
              "end_date": period_end_date - datetime.timedelta(days=1),  # last last Monday
              "electricity_cost": 0,
              "electricity_cost_dollar": 0,
              "other_cost": 0,
              "other_cost_dollar": 0,
              "net_income": r_period['sum_mined'],
              "net_income_dollar": r_period['sum_mined_dollar'],
              "customer_wallet": wallet,
              "customer_miningpool": pool_wallet,
              "num_days": period_days
              }
    creport_email.send_html(report_html(params), to_email,
                            secrets.email_config.default_from_addr,
                            'Crypto Mining Report'
                            )


def all_customer_reports (end_date, period_days):
    customers = pd.read_csv('resources/Customers.csv', delimiter=',')
    download_price = True
    for _, row in customers.iterrows():
        customer_report(row['Wallet'], row['MiningPool'], row['Email'],
                        end_date, period_days,
                        download_price)
        download_price = False


def run_weekly_reports():
    all_customer_reports(datetime.date.today(), 7)


def run_monthly_reports():
    all_customer_reports(datetime.date.today(), 30)


if __name__ == '__main__':
    customer_report(secrets.MY_WALLET,
                    secrets.POOL_WALLET,
                    secrets.email_config.default_to_addr,
                    datetime.date(2021, 3, 15)
                    )
