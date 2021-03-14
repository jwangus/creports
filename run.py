import schedule
import time


def mining_reports():
    print('running mining reports')
    print('completed mining reports')


def etf_sector_momentum_report():
    print('running etf sector momentum report')
    print('completed etf sector momentum report')


def setup_task():
    # schedule.every().monday.at("5:00").do()
    schedule.every(5).seconds.do(mining_reports)
    schedule.every(10).seconds.do(etf_sector_momentum_report)


def run():
    while True:
        schedule.run_pending()
        time.sleep(5)


if __name__ == '__main__':
    setup_task()
    run()
