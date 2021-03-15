import schedule
import time
import mining_report


def mining_reports_weekly():
    print('running weekly reports')
    mining_report.run_weekly_reports()
    print('completed mining weekly reports')


def mining_reports_monthly():
    print('running monthly reports')
    mining_report.run_monthly_reports()
    print('completed mining monthly reports')


def etf_sector_momentum_report():
    print('running etf sector momentum report')
    print('completed etf sector momentum report')


def print_heartbeat():
    print('scheduler is running ok.')


def setup_task():
    schedule.every().monday.at("17:00").do(mining_reports_weekly)
    schedule.every(4).monday.at("17:00").do(mining_reports_monthly())
    #schedule.every(10).seconds.do(etf_sector_momentum_report)
    schedule.every(3).minutes.do(print_heartbeat())


def run():
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == '__main__':
    setup_task()
    run()
