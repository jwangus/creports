import os
import logging
from secedgar import DailyFilings
from secedgar.client import NetworkClient
from secedgar.parser import MetaParser
from creports_utils import previous_weekday
from _secrets import USER_AGENT_EMAIL, SEC_FILINGS_REPO_FOLDER
import xml.etree.ElementTree as ET
import dateutil.parser
import pandas as pd
import numpy as np


def filings_path(report_date):
    return os.path.join(SEC_FILINGS_REPO_FOLDER, f"{report_date:%Y%m%d}")


def download_form4(report_date):
    def _filter_for_form4(filing_entry):
        return filing_entry.form_type.lower() == "4"

    # retry interval in seconds increases  2**n * backoff_factor
    daily_filings = DailyFilings(date=report_date,
                                 client=NetworkClient(
                                     user_agent=USER_AGENT_EMAIL,
                                     backoff_factor=5.0,
                                     rate_limit=5),
                                 entry_filter=_filter_for_form4)
    daily_filings.save(directory=SEC_FILINGS_REPO_FOLDER)


def process_download(report_date):
    """ process all downloaded files for the day
    """
    all_filings = find_all_filing_files(report_date)

    p = MetaParser()
    for f in all_filings:
        p.process(f)


def find_all_filing_files(report_date, file_extension='txt'):
    all_filings = []
    for root, _, filenames in os.walk(filings_path(report_date)):
        for filename in filenames:
            if filename.endswith(f".{file_extension}"):
                all_filings.append(os.path.join(root, filename))
    logging.info(f"number of fillings: {len(all_filings)}")
    return all_filings


def map_relationship(r):
    relation_elements = r.findall(
        "./reportingOwner/reportingOwnerRelationship/")
    relationship = ""
    title = ""
    for e in relation_elements:
        if e.tag.startswith("is") and e.text.lower().strip() in ("1", "yes", "true", "y"):
            relationship = e.tag[2:]
        elif e.tag == "officerTitle":
            title = e.text
    return relationship, title


def parse_form4_xml(s, filename):
    logging.debug(f"reading file: {filename}")
    with open(filename, "r") as f:
        # filter out invalid XML tags
        root = ET.fromstringlist(
            filter(lambda l: not l.startswith(("<XML>", "</XML>")), f.readlines()))
    logging.debug(str(root))

    cik = root.find("./issuer/issuerCik").text
    ticker = root.find("./issuer/issuerTradingSymbol").text
    name = root.find("./reportingOwner/reportingOwnerId/rptOwnerName").text
    relationship, title = map_relationship(root)
    path_root = "./nonDerivativeTable/"
    for ele in root.findall(path_root):
        e_tx_date = ele.find("./transactionDate/value")
        e_tx_code = ele.find("./transactionCoding/transactionCode")
        if e_tx_date is None or e_tx_code is None:
            continue

        s["cik"].append(cik)
        s["ticker"].append(ticker)
        s["name"].append(name)
        s["relationship"].append(relationship)
        s["title"].append(title)
        
        s["tx_date"].append(dateutil.parser.isoparse(e_tx_date.text).date())
        s["tx_code"].append(e_tx_code.text)
        e = ele.find("./transactionAmounts/transactionShares/value")
        s["tx_share"].append(None if e is None else float(e.text))
        e = ele.find("./transactionAmounts/transactionPricePerShare/value")
        s["tx_price"].append(None if e is None else float(e.text))
        v = ele.find("./postTransactionAmounts/sharesOwnedFollowingTransaction/value").text
        s["share_post_tx"].append(float(v))


def generate_daily_summary_report_data(report_date):
    all_form4_xml = filter(lambda l: "form4" in l.lower(
    ), find_all_filing_files(report_date, "xml"))

    s = {"cik": [], "ticker": [], "name": [], "relationship": [], "title": [],
         "tx_date": [], "tx_code": [], "tx_share": [], "tx_price": [], "share_post_tx": []}

    # sometimes, identicle form 4 would appear under owner's CIK and company's CIK.
    # So we want to filter out the duplicate.
    folder_seen=[]
    for filename in all_form4_xml:
        folder_name = os.path.basename(os.path.dirname(filename))
        if folder_name not in folder_seen:
            folder_seen.append(folder_name)
            parse_form4_xml(s, filename)

    return s


def capitalize_word(n):
    donot_cap_words = ["VP", "EVP", "CEO", "II", "III", "CFO"]
    return ' '.join(i if i in donot_cap_words else i.capitalize() for i in n.split())


def format_name_title(name_title):
    comp = name_title.split("/")
    cik = comp[0]
    cell_text = "/".join(comp[1:])
    return f'<a href="https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK={cik}">{cell_text}</a>'


def format_ticker(t):
    return f'<a href="https://finance.yahoo.com/quote/{t}">{t}</a>'


def generate_daily_summary_report(report_data):
    df = pd.DataFrame(report_data)
    df = df[(df.tx_code == "P") | (df.tx_code == "S")]
    df["amt"] = df.tx_share*df.tx_price
    df["sign_pre_share_calc"] = df["tx_code"].map(
        lambda c: 1 if c == "P" else -1)
    df["pre_share"] = df.share_post_tx - df.tx_share * df.sign_pre_share_calc
    df["tx_share_percentage"] = 100*df["tx_share"] / \
        df["pre_share"]*df["sign_pre_share_calc"]

    summary = df.groupby(["cik", "name", "relationship", "title", "ticker", "tx_code", "tx_date", "sign_pre_share_calc"]).agg(
        {"amt": ["sum"], "tx_share_percentage": ["max"], "share_post_tx": ["max"], "tx_price": ["max"]})
    summary.sort_values(["sign_pre_share_calc", ("amt", "sum")],
                        ascending=False, inplace=True)
    summary.reset_index(inplace=True)

    df_output = pd.DataFrame()

    df_output["Name/Title"] = summary["cik"] + "/" + summary["name"].map(
        capitalize_word) + "/" + summary["relationship"] + "/" + summary["title"].map(capitalize_word)
    df_output["Name/Title"] = df_output["Name/Title"].map(format_name_title)

    df_output["Ticker"] = summary["ticker"].map(format_ticker)
    df_output["Buy/Sell"] = summary["tx_code"].map(
        lambda c: "Sell" if c == "S" else "Buy")

    df_output["Trade Date"] = summary["tx_date"].map(str)

    df_output["Trade Amount"] = summary[("amt", "sum")].map('{:,.0f}'.format)
    df_output["% Change In Position"] = summary[(
        "tx_share_percentage", "max")].map(lambda n: '{:,.01f}%'.format(n) if n != np.Inf else 'New')

    df_output["Position Post Trade"] = summary[(
        "share_post_tx", "max")].map('{:,.0f}'.format)
    df_output["Trade Price"] = summary[(
        "tx_price", "max")].map('{:,.2f}'.format)

    return df_output, pd.DataFrame(report_data)