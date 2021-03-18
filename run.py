import datetime
import time

import schedule

import mining_report


def mining_reports():
    today = datetime.date.today()
    print('running weekly reports on: ' + str(today))
    mining_report.run_weekly_reports()

    # if today is the first 6 days of the month, let's run a monthly report
    if today.day < 7:
        print('today is beginning of the month.  Running previous monthly report')
        mining_report.run_monthly_reports()

    print('completed mining reports')


def etf_sector_momentum_report():
    print('running etf sector momentum report')
    print('completed etf sector momentum report')


def print_heartbeat():
    print('scheduler is running ok.')


def setup_task():
    schedule.every().monday.at("19:00").do(mining_reports)
    schedule.every(3).minutes.do(print_heartbeat)


def run():
    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == '__main__':
    setup_task()
    run()
