import logging
import os

from _secrets import BCC_CONGRESS_TRADES, MY_EMAIL, LOG_FOLDER, REPORTS_FOLDER
from creport_email import send_html
from creports_utils import previous_weekday


def create_report(filing_date):
    with open(os.path.join(REPORTS_FOLDER, f"insider_reports_{filing_date}.html"), "r") as f:
        insider_trades_table = f.read()

    with open(os.path.join(REPORTS_FOLDER, f"congress_trades_{filing_date}.html"), "r") as f:
        new_congress_trades_table = f.read()

    return f"""\
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    table, th, td {{
        border: 1px solid black;
        border-collapse: collapse;
    }}
    th, td {{
        padding: 5px;
        text-align: left;
    }}
    </style>
    </head>
    {new_congress_trades_table}
    <hr>
    {insider_trades_table}
    <p>The daily report is run at 9:05am on a weekday.  Let me know if you want to be removed.  Thank you and happy trading!</p>
    </body>
    </html>
    """


if __name__ == "__main__":
    # comment it out after testing
    BCC_CONGRESS_TRADES = ""

    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'email_reports.log'),
                        format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.info('===> Sending emails. Number of emails in the bcc %s',
                 str(len(BCC_CONGRESS_TRADES)))
    filing_date = previous_weekday()
    report_html = create_report(filing_date)
    filename = os.path.join(REPORTS_FOLDER, f"sod_report_{filing_date}.html")

    logging.info(f"Saving report: {filename}")
    with open(filename, "w") as f:
        f.write(report_html)

    send_html(report_html, MY_EMAIL, MY_EMAIL,
              'Congress + Insider Trade Reports', BCC_CONGRESS_TRADES)
    logging.info('===> Sending email done.')
