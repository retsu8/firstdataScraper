import csv
import glob
import math
import os
import pandas as pd
import re
import shutil
import sys
import time
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from selenium.common.exceptions import NoAlertPresentException, NoSuchElementException, UnexpectedAlertPresentException, StaleElementReferenceException
from decimal import Decimal, getcontext
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pdf2txt import main as pd2t
from pdfminer.pdfparser import PDFSyntaxError
from drive import Drive

direct = os.getcwd()
dwn = os.path.join(direct, "pdf")
now = datetime.now()
rolling_12_cycle = now - relativedelta(years=1)

firstdata = {
    "drivername": "mysql+pymysql",
    "host": "merchdb.c0v9kpl8n2zi.us-west-2.rds.amazonaws.com",
    "port": 3306,
    "username": "merch_admin",
    "password": "!GrKDb04gioSVQ*A2c$2",
    "database": "statementData",
    "query": {"ssl_ca": "./rds-combined-ca-bundle.pem"}
}

card_dict = {
    "vi": "Visa",
    "ae": "Amex",
    "mc": "MasterCard",
    "dv": "Discover",
    "eb": "EBT",
    "db": "PinDebit"
}

folderID = "0B7PSHsdd0u-CblM0SlBvcmZwQVk"


def firstConn():
    """Connect to firstdata db"""
    firsty = create_engine(URL(**firstdata), strategy="threadlocal")
    firsty.echo = False
    return firsty


def callConnection(conn, sql):
    # open a transaction - this runs in the context of method_a's transaction
    trans = conn.begin()
    try:
        inst = conn.execute(sql)
        trans.commit()  # transaction is not committed yet
    except:
        print(sys.exc_info())
        inst = sys.exc_info()
        trans.rollback()  # this rolls back the transaction unconditionally
        raise
    return inst


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    if len(l) % n == 0:
        r = int(len(l) / n)
    else:
        r = int(math.ceil(len(l) / n))
    for i in range(0, len(l), r):
        yield l[i:i + r]


def to_type(x):
    y = x.lower()
    if y in card_dict:
        return card_dict[y]
    else:
        return "Other"


def to_int(x):
    y = x.replace(",", "")
    try:
        y = float(y)
        y = int(y)
    except:
        print(sys.exc_info)
    return y


def to_cent(x):
    getcontext().prec = 6
    y = str(x)
    y = y.replace("$", "").replace(",", "")
    try:
        y = y.replace("(", "").replace(")", "")
    except:
        """Justing holding this up"""
    y = Decimal(y)
    return int(y * 100)


def cleanhtml(raw_html):
    cleanr = re.compile("<.*?>")
    return re.sub(cleanr, "", raw_html)


def empty_folder(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)
            return False
    return True


def manage_firstdata(method):
    def decorated_method(self, *args, **kwargs):
        # Befor the method call do this
        self.first = firstConn()
        self.confir = self.first.connect()

        # actual method call
        result = method(self, *args, **kwargs)

        # after the method call
        self.confir.close()

        return result
    return decorated_method


class Main(object):
    @manage_firstdata
    def get_ID(self, mid, date, statement):
        dr = Drive()
        mn = Main()
        print(mid, date, statement)
        currentdate = date.replace(day=1)
        headers = ["MID", "date", "fileID", "folderID"]
        sect = {}
        try:
            my_query = "SELECT * from statementData.notifyStatement where MID like '{}' and `date` = '{}' limit 1".format(
                "','".join(mid), str(currentdate.date()))

            print(my_query)
            fileID = [item for item in callConnection(self.confir, my_query)]
            print("This is my search for file id: {}".format(fileID))
            if not fileID:
                my_query = "SELECT * from notifyStatement where MID = '{}' limit 1".format(
                    "','".join(mid))
                midID = [item for item in callConnection(
                    self.confir, my_query)]
                print(midID)
                if midID:
                    midID = midID[0]
                    print("all get the folder than " + str(midID))

                    fileID = dr.upload(statement, str(date.date()), midID[3])
                    print("This is my file" + fileID)
                    if fileID:
                        ins = "INSERT into notifyStatement({}) Values('{}')".format(
                            "`mid`,`date`,`fileID`,`folderID`", "','".join([mid[0], str(currentdate), fileID, folderID]))
                        print(ins)
                        if callConnection(self.confir, ins):
                            sect["success"] = {"MID": mid, "date": str(
                                currentdate), "fileID": fileID, "folderID": midID[3]}

                    else:
                        respo = "The current month does not exist yet for {}".format(
                            mid[0])
                        print(respo)
                        sect["error"] = respo

                else:
                    print("creating the folder from :" + str(mid[0]), folderID)
                    midID = dr.createFolder(str(mid[0]), folderID)
                    print("This is my mid :" + midID)
                    name = "{}.pdf".format(str(date.date()))
                    fileID = dr.upload(statement, name, midID)
                    print("This is my file :" + fileID)
                    if fileID:
                        ins = "INSERT into notifyStatement({}) Values('{}')".format(
                            "`mid`,`date`,`fileID`,`folderID`", "','".join([mid[0], str(currentdate), fileID, midID]))
                        print(ins)
                        if callConnection(self.confir, ins):
                            sect["success"] = {"MID": mid, "date": str(
                                currentdate), "fileID": fileID, "folderID": midID}

                    else:
                        respo = "The current month does not exist yet for {}".format(
                            mid[0])
                        print(respo)
                        sect["error"] = respo

            else:
                fileID = [str(i) for i in list(fileID[0])]
                sect["success"] = dict(zip(headers, fileID))
        except:
            print(sys.exc_info())
            sect["error"] = sys.exc_info()
            raise

        return sect

    @manage_firstdata
    def parse_statement(self):
        mn = Main()
        month = now.month
        year = now.year
        if now.month < 2:
            month = 12
            year -= 1
        else:
            month -= 1
        for file in os.listdir("pdf"):
            print(month)
            print(year)
            my_file = re.split("_", file)
            new_yr = my_file[3].split(".")[0]
            if not month == my_file[2] and year == new_yr:
                os.unlink(os.path.join(dwn, file))
                continue
            open_file = os.path.join(dwn, file)
            # print(open_file)
            try:
                pd2t([open_file, "-t", "html", "-o", "temp.html"])
            except PDFSyntaxError:
                continue
            original = [item for item in open("temp.html", "r")]
            my_statement_dict = {}
            cleanup = []
            for line in original:
                cleanup.append(cleanhtml(line.strip()).lower())
            my_file = cleanup
            my_file = list(filter(None, my_file))
            #[print(item) for item in my_file]
            sec_amt_submit = False
            for i, line in enumerate(my_file):
                if "this is not a bill" in line and "proccessing_date" not in my_statement_dict:
                    proccessing_date = i + 1
                    my_statement_dict["proccessing_date"] = datetime.strptime(
                        my_file[proccessing_date][-8:], "%m/%d/%y")

                if "amounts submitted" in line and sec_amt_submit and "amounts_submitted_table" not in my_statement_dict:
                    amounts_submitted_table = i
                    my_statement_dict["amounts_submitted_table"] = amounts_submitted_table

                if "merchant number" in line and "mid" not in my_statement_dict:
                    merch_number = my_file.index(line)
                    my_statement_dict["mid"] = str(
                        my_file[merch_number + 4].replace(" ", ""))

                elif "amounts submitted" in line and "amount_submitted" not in my_statement_dict:
                    sec_amt_submit = True
                    amount_submitted = i
                    my_statement_dict["amount_submitted"] = str(my_file[amount_submitted + 6])

                elif "third party transactions" in line and "third_party_transactions" not in my_statement_dict:
                    third_party_transactions = my_file.index(line)
                    my_statement_dict["third_party_transactions"] = str(my_file[amount_submitted + 6])

                elif "adjustments/chargebacks" in line and "adjustments_chargebacks" not in my_statement_dict:
                    adjustments_chargebacks = i
                    # print(adjustments_chargebacks)
                    # print(my_file[adjustments_chargebacks+6])
                    my_statement_dict["adjustments_chargebacks"] = str(my_file[adjustments_chargebacks + 6])

                elif "fees charged" in line and "fees_charged" not in my_statement_dict:
                    fees_charged = i
                    my_statement_dict["fees_charged"] = str(
                        my_file[fees_charged + 6])

                elif "summary by card type" in line and "summary_card_type" not in my_statement_dict:
                    summary_card_type = i
                    my_statement_dict["summary_card_type"] = ""

                elif "amounts funded by batch" in line and "batch_fund" not in my_statement_dict:
                    batch_fund = my_file.index(line)
                    my_statement_dict["batch_fund"] = batch_fund

                elif "no third party transactions for this statement period" in line and "fee_charg" not in my_statement_dict:
                    fee_charg = i + 1
                    my_statement_dict["fee_charg"] = fee_charg

                elif "fee type legend" in line and "fee_type_legend" not in my_statement_dict:
                    fee_type_legend = i
                    my_statement_dict["fee_type_legend"] = fee_type_legend

            headers = "`,`".join(["date", "mid", "sale_count", "sale_vol",
                                  "refund_count", "refund_vol", "debit_count", "total_chr"])
            lst = [my_statement_dict["proccessing_date"].date(),
                   my_statement_dict["mid"]]

            if float(to_cent(my_statement_dict["amount_submitted"])) > 0:
                sum_by_card = my_file[summary_card_type + 3:batch_fund]
                # print(sum_by_card)
                ticket_loc = sum_by_card.index("items")
                sum_by_card = sum_by_card[:ticket_loc] + \
                    [0] + sum_by_card[ticket_loc:]
                z = ["amount", "ticket", "average", "items", "total gross sales you submitted",
                     "refunds", "total amount you submitted", "card type", "(wex)"]
                sum_by_card = [x for x in sum_by_card if x not in z]
                six_by = [item for item in chunks(sum_by_card, 7)]
                # print(six_by)
                six_headers = six_by.pop(0)
                my_panda = pd.DataFrame(six_by, columns=six_headers, index=[
                                        "avg_ticket", "gross_sale_items", "gross_sale_amt", "refund_items", "refund_amount", "total_amount"])
                my_statement_dict["summary_card_type"] = my_panda

                # print(my_panda)

                amt_fnd_btch = my_file[batch_fund +
                                       4:amounts_submitted_table - 11]
                ticket_loc = amt_fnd_btch.index("batch")
                amt_fnd_btch = amt_fnd_btch[:ticket_loc] + \
                    [0, 0] + amt_fnd_btch[ticket_loc:]

                ticket_loc = amt_fnd_btch.index("month end charge")
                amt_fnd_btch = amt_fnd_btch[:ticket_loc] + \
                    [0, 0] + amt_fnd_btch[ticket_loc:]

                z = ["total", "batch", "number", "month end charge", "submitted", "amount", "third party",
                     "transactions", "adjustments/", "chargebacks", "fees", "charged", "funded", "(wex)"]
                amt_fnd_btch = [x for x in amt_fnd_btch if x not in z]
                six_by = [item for item in chunks(amt_fnd_btch, 7)]

                six_headers = six_by.pop(0)
                my_panda = pd.DataFrame(six_by, columns=six_headers, index=["batch_number", "sub_amt", "third_party_trans", "adj_charge", "fees_charge", "funded_amt"])
                my_statement_dict["amt_fnd_btch"] = my_panda

                # print(my_statement_dict["summary_card_type"])

                # print(my_panda)
                # print(my_statement_dict)
                del my_statement_dict["summary_card_type"]["total"]
                print(my_statement_dict["summary_card_type"])
                my_statement_dict["summary_card_type"] = my_statement_dict["summary_card_type"].transpose(
                )
                my_statement_dict["summary_card_type"]["gross_sale_items"] = my_statement_dict["summary_card_type"]["gross_sale_items"].apply(
                    to_int)
                # print(my_statement_dict["summary_card_type"]["gross_sale_items"])
                lst.append(my_statement_dict["summary_card_type"]["gross_sale_items"].sum())
                my_statement_dict["summary_card_type"]["gross_sale_amt"] = my_statement_dict["summary_card_type"]["gross_sale_amt"].apply(
                    to_cent)
                lst.append(my_statement_dict["summary_card_type"]["gross_sale_amt"].sum())

                my_statement_dict["summary_card_type"]["refund_items"] = my_statement_dict["summary_card_type"]["refund_items"].apply(
                    to_int)
                lst.append(
                    my_statement_dict["summary_card_type"]["refund_items"].sum())

                my_statement_dict["summary_card_type"]["refund_amount"] = my_statement_dict["summary_card_type"]["refund_amount"].apply(
                    to_cent)
                lst.append(
                    my_statement_dict["summary_card_type"]["refund_amount"].sum())

                my_statement_dict["summary_card_type"]["gross_sale_amt"] = my_statement_dict["summary_card_type"]["gross_sale_amt"].apply(
                    to_cent)
                lst.append(
                    my_statement_dict["summary_card_type"]["gross_sale_amt"].sum())

                lst.append(my_statement_dict["adjustments_chargebacks"])

            else:
                lst.extend([0, 0, 0, 0, 0, 0])
                print(lst)

            insert_lst = "','".join(str(x) for x in lst)
            insert_me = "insert into firstdata_statement(`{}`) VALUES('{}')".format(headers, insert_lst)

            moved_pdf = "done/{}{}.pdf".format(
                str(my_statement_dict["proccessing_date"].date()), str(my_statement_dict["mid"]))
            moved_pdf = os.path.join(direct, moved_pdf)
            if not os.path.exists("done"):
                os.makedirs("done")
            shutil.move(open_file, moved_pdf)
            if callConnection(self.confir, insert_me):
                mn.get_ID([my_statement_dict["mid"]], my_statement_dict["proccessing_date"], moved_pdf)

            else:
                print("Failed to insert the value, I already exists {}".format(
                    sys.exc_info))

    @manage_firstdata
    def get_mid_search(self):
        """Get all currently processing mids from transactions"""
        my_mid = pd.read_sql("SELECT distinct mid from firstdata.transactions where `processing-date` > '{}'".format(rolling_12_cycle), self.confir)
        return my_mid["mid"].tolist()

    def getstatement(self):
        from selenium import webdriver
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.common.action_chains import ActionChains
        from pyvirtualdisplay import Display
        from driver_builder import DriverBuilder

        """Setup options for chrome web browser"""
        mn = Main()
        mid_to_search = mn.get_mid_search()
        display = Display(visible=0, size=(800, 600))
        display.start()

        driver_builder = DriverBuilder()
        self.browser = driver_builder.get_driver(dwn, headless=False)
        browser = self.browser

        browser.get("https://www.youraccessone.com")

        username = browser.find_element_by_id("txtUserName")
        password = browser.find_element_by_id("txtPassword")

        username.send_keys(os.environ["username"])
        password.send_keys(os.environ["password"])

        browser.find_element_by_name("uxLogin").click()
        # go to chargebacks
        browser.get("https://www.youraccessone.com/64_rpt_Statements.aspx")

        # open filter
        browser.find_element_by_xpath("//*[text()='FILTER']").click()

        for item in mid_to_search:
            # check my merchant
            print(item)
            for x in range(5):
                try:
                    browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxReportFilter_ctl00").click()
                    break
                except NoSuchElementException:
                    time.sleep(2)

            time.sleep(2)
            browser.find_element_by_xpath("//*[text()='Merchant Number']").click()
            time.sleep(.5)
            mid = browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxReportFilter_inMERCHANTNUMBER")
            mid.clear()
            mid.send_keys(item)

            # Click search
            time.sleep(.5)
            try:
                browser.find_element_by_name("ctl00$ContentPage$uxFiltering$uxReportFilter$btSubmit").click()
                my_val = browser.find_element_by_name("ctl00$ContentPage$uxdroplist").get_attribute("value")
                my_val = datetime.strptime(my_val, '%m/%d/%Y')
                now = datetime.now() - relativedelta(months=1)
                print(my_val.month, now.month, my_val.year, now.year)
                if my_val.month == now.month and my_val.year == now.year:
                    browser.find_element_by_name("ctl00$ContentPage$uxSearch").click()
            except NoSuchElementException:
                continue

        browser.quit()
        try:
            display.close()
        except AttributeError:
            print("The Display is already closed?")

if __name__ == "__main__":
    import argparse
    mn = Main()
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--run", help="Run the selenium script to grab csv", action="store_true", default=False)
    parser.add_argument("-p", "--parse", help="Parse the csv", action="store_true", default=False)
    parser.add_argument("-d", "--date", help="Check the date to see if statements need to run", action="store_true", default=False)
    args = parser.parse_args()
    if args.date:
        if 5 < now.day:
            sys.exit()
    if args.run:
        mn.getstatement()
    if args.parse:
        mn.parse_statement()
