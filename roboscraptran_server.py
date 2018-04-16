#! env/bin/python

import argparse
import csv
import datetime
import glob
import os
import pandas as pd
import requests
import shutil
import string
import sys
import time
import urllib
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from pyvirtualdisplay import Display
from driver_builder import DriverBuilder
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from random import choice, randint
from decimal import Decimal, getcontext

now = datetime.datetime.now()
direct = os.getcwd()
dwn = os.path.join(direct, 'csv')

firstdata = {
    'drivername': 'mysql+pymysql',
    'host': 'merchdb.c0v9kpl8n2zi.us-west-2.rds.amazonaws.com',
    'port': 3306,
    'username': 'merch_admin',
    'password': '!GrKDb04gioSVQ*A2c$2',
    'database': 'firstdata',
    'query': {'local_infile': 1, 'ssl_ca': './rds-combined-ca-bundle.pem'},
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


def generate(mini=170, maxi=250):
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
    size = randint(mini, maxi)
    return ''.join(choice(chars) for _ in range(size))


def firstConn():
    """Connect to firstdata db"""
    firsty = create_engine(URL(**firstdata), strategy='threadlocal')
    firsty.echo = False
    return firsty


def to_cent(x):
    getcontext().prec = 6
    y = x.replace('$', '').replace(',', '')
    try:
        y = y.replace('(', '').replace(')', '')
    except:
        """Justing holding this up"""
    y = Decimal(y)
    return int(y * 100)


def to_type(x):
    y = x.lower()
    if y in card_dict:
        return card_dict[y]
    else:
        return 'Other'


def key_bit(x):
    y = x.lower()
    if y in 'y':
        return 1
    else:
        return 0


def to_countrycode(x):
    x = x.lower()
    if x in country_codes:
        return country_codes[x]


def make_type_nice(x):
    if 'R' == x or 'RTRN' == x:
        return str('Refund')
    else:
        return str('Sale')


def key_bit(x):
    y = x.lower()
    if y in 'y':
        return 1
    else:
        return 0


def build_tran_id(my_panda):
    return str(my_panda['reference-number']) + " " + str(my_panda['File Source']) + ' ' + str(generate(10, 100))


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


def buildhttps(login, username, password):
    request = urllib.request
    cj = CookieJar()
    opener = request.build_opener(request.HTTPCookieProcessor(cj))
    values = {'username': username, 'password': password}
    data = urllib.parse.urlencode(values).encode("utf-8")
    response = opener.open(login, data)
    request.install_opener(opener)
    return opener


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

##
# Get info from db
##


def callConnection(conn, sql):
    # open a transaction - this runs in the context of method_a's transaction
    trans = conn.begin()
    try:
        inst = conn.execute(sql)
        trans.commit()  # transaction is not committed yet
    except:
        print(sql)
        print(sys.exc_info())
        inst = sys.exc_info()
        trans.rollback()  # this rolls back the transaction unconditionally
        raise
    return inst


class Main(object):

    @manage_firstdata
    def parse_csv(self, server):
        """Parse the csv pulled from the transactions table"""
        i = 0
        for file in os.listdir("csv"):
            my_file = os.path.join(dwn, file)
            if file.endswith(".csv") and '_panda' not in my_file:
                print(my_file)
                try:
                    my_panda = pd.read_csv(my_file, header=1, parse_dates=[
                                           4, 5], infer_datetime_format=True, encoding='utf-8')
                except pd.errors.ParserError:
                    print('invalid file')
                    os.unlink(my_file)
                    continue
                #my_panda.reset_index(level=0, inplace=True)
                del my_panda['Matched']
                del my_panda['Exp Date']
                my_panda.rename(columns={'Keyed': 'keyed', 'Merchant #': 'MID', 'Merchant Name': 'merchant-name', 'Report Date': 'processing-date', 'Trans Date': 'transaction-date', 'Batch #': 'batch-number', 'Trans Time': 'transaction-time',
                                         'Trans Code': 'tran-type', 'Country Code': 'country-code', 'Card Type': 'card-type', 'Card #': 'card-number', 'Auth #': 'auth-code', 'Trans Amount': 'amount', 'Terminal #': 'reference-number'}, inplace=True)
                my_panda = my_panda.fillna(0)

                my_date = str(my_panda.iloc[0]['processing-date']).replace('/', '-')
                my_panda['card-type'] = my_panda['card-type'].apply(to_type)
                my_panda['country-code'] = my_panda['country-code'].apply(
                    to_countrycode)
                my_panda['tran-type'] = my_panda['tran-type'].apply(
                    make_type_nice)
                my_panda['processing-date'] = pd.to_datetime(
                    my_panda['processing-date'], format="%m/%d/%Y").astype('datetime64[ns]', copy=True)
                my_panda['transaction-date'] = pd.to_datetime(
                    my_panda['transaction-date'], format='%m/%d/%Y').astype('datetime64[ns]', copy=True)
                my_panda['batch-date'] = pd.to_datetime(
                    my_panda['transaction-date'])
                my_panda['tran-identifier'] = my_panda.apply(
                    build_tran_id, axis=1)
                my_panda['keyed'] = my_panda['keyed'].apply(key_bit)
                del my_panda['File Source']
                my_panda['amount'] = my_panda['amount'].apply(to_cent)
                my_panda.sort_values(by=['processing-date'], inplace=True)

                headers = '`,`'.join(list(my_panda))
                try:
                    my_panda.to_sql('transactions', self.confir,
                                    if_exists='append', index=False, chunksize=1000)
                except:
                    m = 0
                    n = 100
                    if n > int(my_panda.shape[0]):
                        n = int(my_panda.shape[0])
                    # print(my_panda.shape)
                    while n <= int(my_panda.shape[0]):
                        update_list = []
                        for j, item in my_panda[m:n].iterrows():
                            """Place in the update script"""
                            update = '","'.join(str(k)
                                                for k in item.values.tolist())
                            update_list.append('("{}")'.format(update))
                        # print(update_list)
                        insert_me = 'insert into transactions(`{}`) VALUES{}'.format(
                            headers, ','.join(update_list))
                        # print(insert_me)
                        if not callConnection(self.confir, insert_me):
                            for j, item in my_panda[m:n].iterrows():
                                """Place in the update script"""
                                update = '","'.join(str(k)
                                                    for k in item.values.tolist())
                                del item['tran-identifier']
                                str_update = ','.join('`{}`="{}"'.format(
                                    key, item) for key, item in item.items())
                                insert_me = 'insert into transactions(`{}`) VALUES("{}") ON DUPLICATE KEY UPDATE {}'.format(
                                    headers, update, str_update)
                                # print(insert_me)
                                if not callConnection(self.confir, insert_me):
                                    print("Something went wronge")
                                    break

                        if n == my_panda.shape[0]:
                            break
                        elif n + 100 > my_panda.shape[0]:
                            n = my_panda.shape[0]
                            m += 100
                        else:
                            n += 100
                            m += 100

                print(my_panda)
            # os.remove(str(my_file)+'_panda.csv')
            if not os.path.exists('done'):
                os.makedirs('done')
            try:
                shutil.move(my_file, 'done/' + str(my_date) + 'tran.csv')
                i++
            except UnboundLocalError:
                os.remove(my_file)
                continue
        if i == 0:
            if os.listdir("csv")=="" and server:
                print("nothing processed lets retry")
                mn = Main()
                mn.gettransactions()
                mn.parse_csv(True)

    def gettransactions(self):
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

        # go to transactions
        browser.get(
            'https://www.youraccessone.com/64_rpt_TransactionSearch.aspx')

        # open filter
        browser.find_element_by_xpath("//*[text()='FILTER']").click()

        # toggle daily
        browser.find_element_by_xpath("//*[text()='Daily']").click()

        # check all merchants
        browser.find_element_by_id("ctl00_ContentPage_uxHierarchyList_Input").click()
        time.sleep(2)
        browser.find_element_by_xpath("//*[text()='SYS']").click()

        # fill in the field
        my_sys = browser.find_element_by_name("ctl00$ContentPage$uxEntityValue")
        my_sys.send_keys('1261')

        # submit to apply filters
        browser.find_element_by_id("ctl00_ContentPage_uxSearch").click()

        time.sleep(10)

        # download transaction
        count = 0
        element_to_hover_over = browser.find_element_by_xpath(
            "//*[text()='EXPORT']")
        hover = ActionChains(browser).move_to_element(element_to_hover_over)
        hover.perform()

        browser.find_element_by_xpath("//*[text()='CSV']").click()
        time.sleep(2)

        for file in os.listdir("csv"):
            my_file = os.path.join(dwn, file)
            if file.endswith(".csv"):
                fileObject = csv.reader(my_file)
                row_count = sum(1 for row in fileObject)
                if row_count > 1:
                    break

        browser.quit()
        display.stop()


if __name__ == '__main__':
    mn = Main()
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--run", help="Run the selenium script to grab csv",
                        action='store_true', default=False)
    parser.add_argument("-p", "--parse", help="Parse the csv",
                        action='store_true', default=False)
    parser.add_argument("-s", "--server", help="Run as server",
                        action='store_true', default=False)
    args = parser.parse_args()
    if args.run:
        mn.gettransactions()
    if args.parse:
        mn.parse_csv(args.server)
