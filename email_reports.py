import logging
import os

from _secrets import BCC_CONGRESS_TRADES, MY_EMAIL, LOG_FOLDER
from creport_email import send_html

new_congress_trades_html_table = ""

html_text = f"""\
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
<h3>Congress Disclosed New Trades</h3>
{new_congress_trades_html_table}
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
    send_html(html_text, MY_EMAIL, MY_EMAIL,
              'Congress and Insider Trades', BCC_CONGRESS_TRADES)
    logging.info('===> Sending email done.')
