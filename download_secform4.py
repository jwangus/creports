import logging
import os
from _secrets import LOG_FOLDER
from secfilings import download_form4, previous_weekday

if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'download_secform4.log'),
                        format='%(asctime)s - %(message)s', level=logging.INFO)
    report_date = previous_weekday()
    logging.info(f"Downloading file for date: {report_date}")

    try:
        download_form4(report_date)
    except Exception as e:
        logging.exception(str(e))
    else:
        logging.info("Download Completed.")
