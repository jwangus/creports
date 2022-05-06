from secfillings import generate_daily_summary
from creports_utils import previous_weekday
from _secrets import LOG_FOLDER
from _secrets import REPORTS_FOLDER
import logging, os
import pandas as pd

if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'generate_daily_inside_tx_summary.log'),
                        format='%(asctime)s - %(message)s', level=logging.DEBUG)
    report_date = previous_weekday()
    logging.info(f"generate daily inside trading summray file for date: {report_date}")

    try:
        generate_daily_summary(report_date)


    except Exception as e:
        logging.exception(str(e))
    else:
        logging.info("generate daily inside trading summray file Completed.")
