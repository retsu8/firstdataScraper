import time, sys, os, csv
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

direct = os.getcwd()
dwn = os.path.join(direct, 'csv')

firstdata = {
    'drivername': 'mysql+pymysql',
    'host': 'merchdb.c0v9kpl8n2zi.us-west-2.rds.amazonaws.com',
    'port': 3306,
    'username': 'merch_admin',
    'password': '!GrKDb04gioSVQ*A2c$2',
    'database': 'druporta_tss_data',
}

card_dict = {
    'VI':'Visa',
    'AE':'Amex',
    'MC':'MasterCard',
    'DV':'Discover',
    'EB':'EBT',
    'DB':'PinDebit'
}

def firstConn():
    """Connect to firstdata db"""
    firsty = create_engine(URL(**firstdata), strategy='threadlocal')
    firsty.echo = False
    return firsty

def to_int(x):
    return int(x)

def to_cent(x):
    try:
        y = float(x)
        return int(y*100)
    except:
        y = x.replace('(', '').replace(')', '')
        y = float(y)
        y = y - y*2
        return int(y*100)

def to_type(x):
    if x in card_dict:
        return card_dict[x]
    else:
        return 'Other'

def to_countrycode(x):
    x = x.lower()
    if 'use' in x:
        return int(840)
    elif 'kor' in x:
        return int(82)

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
    def parse_csv(self):
        for file in os.listdir("csv"):
            my_file = os.path.join(dwn, file)
            if file.endswith(".csv"):
                print(my_file)
                my_panda = pd.read_csv(my_file, header=1, parse_dates=True, infer_datetime_format=True)
                #my_panda.reset_index(level=0, inplace=True)
                del my_panda['Matched']
                del my_panda['Terminal #']
                del my_panda['File Source']
                del my_panda['Keyed']
                del my_panda['Exp Date']
                my_panda.rename(columns={'Merchant #':'mid','Merchant Name':'merchant-name','Report Date': 'processing-date', 'Trans Date': 'transaction-date', 'Batch #':'batch-number', 'Trans Time':'tran-time','Trans Code':'tran-type', 'Country Code':'country-code','Card Type':'card-type','Card #':'card-number','Auth #':'auth-code','Trans Amount':'amount'}, inplace=True)
                my_panda= my_panda.fillna(0)
                my_panda['card-type'] = my_panda['card-type'].apply(to_type)
                my_panda['country-code'] = my_panda['country-code'].apply(to_countrycode)
                my_panda['processing-date'] = pd.to_datetime(my_panda['processing-date']).astype('datetime64[ns]', copy=True)
                my_panda['transaction-date'] = pd.to_datetime(my_panda['transaction-date']).astype('datetime64[ns]', copy=True)
                my_panda['amount'] = my_panda['amount'].replace('[\$,]', '', regex=True).apply(to_cent)
                #my_panda.to_sql('transactions', self.confir, if_exists='append', index=False)
                print(my_panda)

    def gettransactions(self):
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.manager.showWhenStarting", False)
        profile.set_preference("browser.download.dir", dwn)
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

        browser = webdriver.Firefox(firefox_profile=profile)
        browser.get('https://www.youraccessone.com')

        username = browser.find_element_by_id("txtUserName")
        password = browser.find_element_by_id("txtPassword")

        username.send_keys(os.environ['username'])
        password.send_keys(os.environ['password'])

        browser.find_element_by_name("uxLogin").click()

        #go to chargebacks
        browser.get('https://www.youraccessone.com/64_rpt_TransactionSearch.aspx')

        #open filter
        browser.find_element_by_xpath("//*[text()='FILTER']").click()

        #toggle daily
        browser.find_element_by_xpath("//*[text()='Daily']").click()

        #check all merchants
        browser.find_element_by_id("ctl00_ContentPage_uxHierarchyList_Input").click()
        browser.find_element_by_xpath("//*[text()='SYS']").click()

        #fill in the field
        my_sys = browser.find_element_by_name("ctl00$ContentPage$uxEntityValue")
        my_sys.send_keys('1261')

        #submit to apply filters
        browser.find_element_by_id("ctl00_ContentPage_uxSearch").click()

        time.sleep(5)

        #download transaction
        browser.find_element_by_xpath("//*[text()='EXPORT']").click()
        browser.find_element_by_id("ctl00_ContentPage_uxExporter_imgCSV").click()

        browser.close();

if __name__ == '__main__':
    mn = Main()
    #mn.gettransactions()
    mn.parse_csv()
