import logging
import os
from _secrets import LOG_FOLDER
from secfilings import process_download, previous_weekday

if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'process_secform4.log'),
                        format='%(asctime)s - %(message)s', level=logging.INFO)
    report_date = previous_weekday()
    logging.info(f"process file for date: {report_date}")

    try:
        process_download(report_date)
    except Exception as e:
        logging.exception(str(e))
    else:
        logging.info("Process daily SEC Form 4 fillings Completed.")
