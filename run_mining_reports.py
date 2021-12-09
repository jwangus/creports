from datetime import date
import time

import schedule

import hqms_report
import mining_report


def mining_reports():
    today = date.today()
    print('running weekly reports on: ' + str(today))
    mining_report.run_weekly_reports()

    # if today is the first 6 days of the month, let's run a monthly report
    if today.day < 7:
        print('today is beginning of the month.  Running previous monthly report')
        mining_report.run_monthly_reports()

    print('completed mining reports')


if __name__ == '__main__':
    mining_reports()
