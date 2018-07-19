import argparse
import csv
import datetime
import os
import pandas as pd
import shutil
import sys
import time
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from pyvirtualdisplay import Display
from driver_builder import DriverBuilder
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from decimal import Decimal, getcontext

now = datetime.datetime.now()
rolling_12_cycle = now - relativedelta(years=1)
direct = os.getcwd()
dwn = os.path.join(direct, 'csv')

firstdata = {
    'drivername': 'mysql+pymysql',
    'host': os.environ["host"],
    'port': os.environ["port"],
    'username': os.environ["rds_user"],
    'password': os.environ["rds_pass"],
    'database': "firstdata",
    'query': {'local_infile': 1, 'ssl_ca': '../rds-combined-ca-bundle.pem'},
}

card_dict = {
    'vi': 'Visa',
    'ae': 'Amex',
    'mc': 'MasterCard',
    'dv': 'Discover',
    'eb': 'EBT',
    'db': 'PinDebit'
}

country_codes = {}
country = os.path.join(direct, 'countryCodes.csv')
with open(country) as csvfile:
    reader = csv.reader(csvfile)
    country_codes = {rows[0].lower(): rows[1] for rows in reader}

def firstConn():
    """Connect to firstdata db"""
    firsty = create_engine(URL(**firstdata), strategy='threadlocal')
    firsty.echo = False
    return firsty


def to_cent(x):
    getcontext().prec = 6
    y = str(x).replace('$', '').replace(',', '')
    try:
        y = y.replace('(', '').replace(')', '')
    except:
        """Justing holding this up"""
    y = Decimal(y)
    return int(y * 100)


def to_type(x):
    y = str(x).lower()
    if y in card_dict:
        return card_dict[y]
    else:
        return 'Other'

def callConnection(conn, sql):
    """open a transaction - this runs in the context of method_a's transaction"""
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
    """Main class file"""
    @manage_firstdata
    def parse_csv(self):
        """Parse the csv pulled from the authorizations table"""
        for file in os.listdir("csv"):
            my_file = os.path.join(dwn, file)
            if file.endswith(".csv") and '_panda' not in my_file:
                print(my_file)
                with open(my_file) as csvfile:
                    readCSV = csv.reader(csvfile, delimiter=',')
                    for row in readCSV:
                        my_row = row
                        break
                    try:
                        mid = my_row[0].split(' ')[3].strip()
                        print(mid)
                    except (IndexError, UnboundLocalError):
                        print("No mid to grab from the csv")
                        continue
                    print(mid)

                try:
                    my_panda = pd.read_csv(my_file, header=1, parse_dates=[0, 1, 2], infer_datetime_format=True, encoding='utf-8')
                except pd.errors.ParserError:
                    print('invalid file')
                    #os.unlink(my_file)
                    continue
                my_panda = my_panda.drop(my_panda.index[len(my_panda)-1])
                my_panda.columns = [x.lower().replace(' ', '_').replace('#', 'number') for x in my_panda.columns]
                my_panda = my_panda.fillna(0)

                my_panda['MID'] = str(mid)
                my_panda.rename(columns={'report_date':'processing-date'}, inplace=True)
                my_panda['auth_number'] = my_panda['auth_number'].astype(str)
                my_date = str(my_panda.iloc[0]['processing-date']).replace('/', '-')
                my_panda['processing-date'] = pd.to_datetime(my_panda['processing-date'])
                my_panda['amount'] = my_panda['auth_amount'].apply(to_cent)
                my_panda['trans_amount'] = my_panda['trans_amount'].apply(to_cent)
                my_panda['card_type'] = my_panda['card_type'].apply(to_type)
                del my_panda['auth_amount']


                print(my_panda.dtypes)

                headers = '`,`'.join(list(my_panda))
                #try:
                my_panda.to_sql('authorizations', self.confir, if_exists='append', index=False, chunksize=1000)
                #except:
                #    m = 0
                #    n = 100
                #    if n > int(my_panda.shape[0]):
                #        n = int(my_panda.shape[0])
                #    # print(my_panda.shape)
                #    while n <= int(my_panda.shape[0]):
                #        update_list = []
                #        for j, item in my_panda[m:n].iterrows():
                #            """Place in the update script"""
                #            update = '","'.join(str(k) for k in item.values.tolist())
                #            update_list.append('("{}")'.format(update))
                #        # print(update_list)
                #        insert_me = 'insert into authorizations(`{}`) VALUES{}'.format(headers, ','.join(update_list))
                #        # print(insert_me)
                #        if not callConnection(self.confir, insert_me):
                #            for j, item in my_panda[m:n].iterrows():
                #                """Place in the update script"""
                #                update = '","'.join(str(k) for k in item.values.tolist())
                #                str_update = ','.join('`{}`="{}"'.format(key, item) for key, item in item.items())
                #                insert_me = 'insert into authorizations(`{}`) VALUES("{}") ON DUPLICATE KEY UPDATE {}'.format(
                #                    headers, update, str_update)
                #                # print(insert_me)
                #                if not callConnection(self.confir, insert_me):
                #                    print("Something went wronge")
                #                    break
#
#                        if n == my_panda.shape[0]:
#                            break
#                        elif n + 100 > my_panda.shape[0]:
#                            n = my_panda.shape[0]
#                            m += 100
#                        else:
#                            n += 100
#                            m += 100

                print(my_panda)
            # os.remove(str(my_file)+'_panda.csv')
            if not os.path.exists('done'):
                os.makedirs('done')
            rename = 'done/{}{}authorization.csv'.format(my_date, mid)
            shutil.move(my_file, rename)

    @manage_firstdata
    def get_mid_search(self):
        """Get all currently processing mids from transactions"""
        my_mid = pd.read_sql("SELECT distinct mid from transactions where `processing-date` > '{}'".format(rolling_12_cycle), self.confir)
        return my_mid['mid'].tolist()

    def get_authorizations(self):
        """Get all the authorizations from firstdata for parsing into the firstdata.authorization table"""
        mn = Main()
        mid_to_search = mn.get_mid_search()

        """Get a csv from youraccessone transactions"""
        display = Display(visible=0, size=(800, 600))
        display.start()

        driver_builder = DriverBuilder()
        browser = driver_builder.get_driver(dwn, headless=False)

        browser.get('https://www.youraccessone.com')

        username = browser.find_element_by_id("txtUserName")
        password = browser.find_element_by_id("txtPassword")

        username.send_keys(os.environ['username'])
        password.send_keys(os.environ['password'])

        browser.find_element_by_name("uxLogin").click()

        time.sleep(1)
        # go to authorizations
        element_to_hover_over = browser.find_element_by_xpath(
            "//*[text()='Reports ']")
        hover = ActionChains(browser).move_to_element(element_to_hover_over).move_to_element(browser.find_element_by_xpath(
            "//*[text()='Authorization Log']")).move_to_element(browser.find_element_by_xpath(
                "//*[text()='Authorizations']"))
        hover.click().perform()

        """open filter"""
        browser.find_element_by_xpath("//*[text()='FILTER']").click()

        """toggle daily"""
        #browser.find_element_by_xpath("//*[text()='Daily']").click()

        """This section for date range not currently used change Daily to Date Range then uncomment"""
        """get date range"""
        start = browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxReportFilter_dpFromDate_dateInput")
        start.clear()
        start.send_keys('03-24-2016')

        end = browser.find_element_by_id('ctl00_ContentPage_uxFiltering_uxReportFilter_dpToDate_dateInput')
        end.clear()
        end.send_keys('03-26-2018')

        """check all merchants"""

        for item in mid_to_search:
            # fill in the field
            browser.find_element_by_id(
                "ctl00_ContentPage_uxFiltering_uxReportFilter_ctl00_Input").click()
            time.sleep(2)
            browser.find_element_by_xpath("//*[text()='Merchant Number']").click()
            text_box = browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxReportFilter_inMERCHANTNUMBER")
            text_box.clear()
            text_box.send_keys(item)

            # submit to apply filters
            try:
                browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxReportFilter_btSubmit").click()

                time.sleep(2)

                # download transaction
                count = 0
                element_to_hover_over = browser.find_element_by_xpath(
                    "//*[text()='EXPORT']")
                hover = ActionChains(browser).move_to_element(element_to_hover_over)
                hover.perform()

                browser.find_element_by_xpath("//*[text()='CSV']").click()
                time.sleep(2)

            except NoSuchElementException:
                continue

        browser.quit()
        display.stop()


if __name__ == '__main__':
    """Argument parse location for parsing out all arguments"""
    mn = Main()
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--run", help="Run the selenium script to grab csv",
                        action='store_true', default=False)
    parser.add_argument("-p", "--parse", help="Parse the csv",
                        action='store_true', default=False)
    args = parser.parse_args()
    if args.run:
        mn.get_authorizations()
    if args.parse:
        mn.parse_csv()
