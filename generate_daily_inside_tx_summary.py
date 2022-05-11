from secfilings import generate_daily_summary_report, download_form4, generate_daily_summary_report_data, process_download
from creports_utils import previous_weekday
from _secrets import LOG_FOLDER
from _secrets import REPORTS_FOLDER
import logging
import os
import pandas as pd

if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'generate_daily_inside_tx_summary.log'),
                        format='%(asctime)s %(name)s %(levelname)s - %(message)s', level=logging.INFO)
    logging.getLogger().setLevel(logging.WARN)

    log = logging.getLogger("insider_tx_summary")
    log.setLevel(logging.INFO)

    report_date = previous_weekday()
    log.info(
        f"generate daily inside trading summray file for date: {report_date}")

    try:
        log.info("extracting report data")
        process_download(report_date)
        log.info("createing report data")
        rep_data = generate_daily_summary_report_data(report_date)
        df, df_raw = generate_daily_summary_report(rep_data)

        filename = os.path.join(
            REPORTS_FOLDER, f"insider_reports_{report_date}")

        log.info(f"saveing csv and html files: {filename}")
        df.to_csv(filename+".csv")
        df_raw.to_csv(filename+"_raw.csv")
        with open(filename+".html", "w") as f:
            f.write(df.to_html(escape=False))

    except Exception as e:
        log.exception(str(e))
    else:
        log.info("generate daily inside trading summray file completed.")
