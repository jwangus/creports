import logging
import os
import zipfile
from datetime import date

import pandas as pd
import requests

from _secrets import MY_EMAIL, BCC_CONGRESS_TRADES, REPORTS_FOLDER, LOG_FOLDER
from creport_email import send_html

from creports_utils import previous_weekday


# save the latest master file, FD.txt by appending today date to the name, FD_2021-07-27.txt
# download_zip_file the master file, which has name, trade date and document id and make it current FD.txt
# compare FD.txt and saved FD_2021-07-27.txt and find new trades.
# create the link to disclose pdf file and send it in an email to me.
class CongressStockDisclose:
    resource_dir = 'resources'
    url_base = 'https://disclosures-clerk.house.gov/public_disc'
    # this is the file that's unziped.
    master_file_name = 'FD.txt'

    def __init__(self):
        self._today = date.today()
        year = self._today.year
        self._master_file_url = f'{self.url_base}/financial-pdfs/{year}FD.ZIP'
        self._disclose_pdf_url = f'{self.url_base}/ptr-pdfs/{year}/'

        self._zip_file_path = os.path.join(
            self.resource_dir, f'{year}CongressTrades.zip')
        self._master_file_path = os.path.join(
            self.resource_dir, self.master_file_name)
        self._latest_master_file_path = os.path.join(
            self.resource_dir, f'FD_{self._today}.txt')

    def download_zip_file(self):
        with requests.get(self._master_file_url, stream=True) as r:
            with open(self._zip_file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=16 * 1024):
                    f.write(chunk)

    def unzip_master_file(self):
        name_in_zip = f'{self._today.year}FD.txt'
        with zipfile.ZipFile(self._zip_file_path, 'r') as f:
            f.extract(name_in_zip, self.resource_dir)
        os.rename(os.path.join(self.resource_dir, name_in_zip),
                  self._master_file_path)

    def get_doc_link(self, docId):
        return f'{self.url_base}/ptr-pdfs/{self._today.year}/{docId}.pdf'

    # compare with previous version and find any newly reported trades
    def find_new_trades(self):
        df_current = pd.read_csv(self._master_file_path, sep='\t')
        # filter out any type other than P
        df_current = df_current.loc[df_current['FilingType'] == 'P']
        # add new column
        df_current['DocLink'] = df_current['DocID'].apply(self.get_doc_link)
        # replace NA with blank string
        df_current.fillna('', inplace=True)
        df_current.drop(columns=['Suffix', 'Year'], inplace=True)

        df_latest = pd.read_csv(
            self._latest_master_file_path, sep='\t', index_col='DocID')

        mask = [docId not in df_latest.index for docId in df_current['DocID']]
        return df_current.loc[mask]

    def save_current_master_file(self):
        if os.path.exists(self._master_file_path):
            if os.path.exists(self._latest_master_file_path):
                # the master file has been saved. Don't save it just delete it.
                os.remove(self._master_file_path)
            else:
                os.rename(self._master_file_path,
                          self._latest_master_file_path)


if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'house_trades_app.log'),
                        format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.info('===============>Begin<===============')
    cd = CongressStockDisclose()
    cd.save_current_master_file()
    logging.info('1. current master file saved.')
    cd.download_zip_file()
    logging.info('2. new master file downloaded.')
    cd.unzip_master_file()
    logging.info('3. unzipped new master file.')

    df_new_trades = cd.find_new_trades()
    new_trades_html_table = df_new_trades.to_html()

    report_date = previous_weekday()
    filename = os.path.join(
        REPORTS_FOLDER, f"congress_trades_{report_date}")

    logging.info(f'4. saving file: {filename}.html and {filename}.csv')
    df_new_trades.to_csv(filename+".csv")
    with open(filename+".html", "w") as f:
        f.write(new_trades_html_table)

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
    {new_trades_html_table}
    <p>The daily report is run at 9:05am on a weekday.  Let me know if you want to be removed.  Thank you and happy trading!</p>
    </body>
    </html>
    """
    logging.info('5. Sending emails. Number of emails in the bcc %s',
                 str(len(BCC_CONGRESS_TRADES)))
    send_html(html_text, MY_EMAIL, MY_EMAIL,
              'Congress Disclosed New Trades', BCC_CONGRESS_TRADES)
    logging.info('===============>End<===============')
