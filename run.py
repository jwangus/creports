import datetime
import time

import schedule

import mining_report
import high_quality_momentum
import save_iex_stats


def mining_reports():
    today = datetime.date.today()
    print('running weekly reports on: ' + str(today))
    mining_report.run_weekly_reports()

    # if today is the first 6 days of the month, let's run a monthly report
    if today.day < 7:
        print('today is beginning of the month.  Running previous monthly report')
        mining_report.run_monthly_reports()

    print('completed mining reports')


def momentum_report():
    today = datetime.date.today()
    if today.weekday():
        print('running momentum report')
        high_quality_momentum.calc_hqm_matrix()
        print('saving iex stats to db report')
        save_iex_stats.save_my_symbols_to_db()
        print("all done!")



def print_heartbeat():
    print('scheduler is running ok.')


def setup_task():
    schedule.every().monday.at("19:00").do(mining_reports)
    schedule.every().day.at('20:30').do(momentum_report)

    schedule.every(60).minutes.do(print_heartbeat)


def run():
    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == '__main__':
    setup_task()
    run()
