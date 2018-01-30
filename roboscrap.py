import time, sys, os
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
    'host': 'auroramerchdb.c0v9kpl8n2zi.us-west-2.rds.amazonaws.com',
    'port': 3306,
    'username': 'merch_admin',
    'password': '!GrKDb04gioSVQ*A2c$2',
    'database': 'firstdata',
}

def firstConn():
    """Connect to firstdata db"""
    firsty = create_engine(URL(**firstdata), strategy='threadlocal')
    firsty.echo = False
    return firsty

def to_int(x):
    return int(x)

def to_cent(x):
    return x*100

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
            if file.endswith(".csv"):
                my_file = os.path.join(dwn, file)
                print(my_file)
                my_panda = pd.DataFrame.from_csv(my_file, header=2, index_col='CB Sequence Number', parse_dates=True, infer_datetime_format=True)
                my_panda = my_panda.drop(my_panda.index[len(my_panda)-1])
                #my_panda.reset_index(level=0, inplace=True)
                my_panda.rename(columns={'Report Date': 'processing-date', 'Trans Date': 'transaction-date', 'Trans Date': 'transaction-date', 'File Source': 'file_source', 'Card Type': 'card-type', 'Cardholder Number': 'card-number', 'CB Type': 'tran-type', 'Reason Text': 'reason-desc', 'Disposition': 'disposition', 'Reference Number': 'reference-number', 'CB Sequence Number': 'ID','CB Reason Code':'record-type', 'Reason Text':'message','CB Amount':'amount','1st CB Amount':'1st_amount'}, inplace=True)
                my_panda= my_panda.fillna(0)
                my_panda['processing-date'] = pd.to_datetime(my_panda['processing-date']).astype('datetime64[ns]', copy=True)
                my_panda['transaction-date'] = pd.to_datetime(my_panda['transaction-date']).astype('datetime64[ns]', copy=True)
                my_panda['tran-type'] = my_panda['tran-type'].astype('int', copy=True).astype('str', copy=True)
                my_panda['record-type'] = my_panda['record-type'].astype('int', copy=True).astype('str', copy=True)
                my_panda['1st_amount'] = my_panda['1st_amount'].replace('[\$,]', '', regex=True).astype(float)
                my_panda['1st_amount'] = my_panda['1st_amount'].apply(to_cent).astype(int)
                my_panda['amount'] = my_panda['amount'].apply(to_cent).astype(int)
                print(my_panda)
                my_panda.to_sql('chargebacks', self.confir, if_exists='replace', index=False)

    def getchargeback():
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
        browser.get('https://www.youraccessone.com/64_rpt_Chargebacks.aspx')

        #open filter
        browser.find_element_by_xpath("//*[text()='FILTER']").click()

        #toggle daily
        browser.find_element_by_xpath("//*[text()='Daily']").click()

        #check all merchants
        browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxOmahaHierarchies_uxchbAllMerchants").click()

        #submit to apply filters
        browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxbtnDisplayRunReport").click()

        time.sleep(5)

        window_before = browser.window_handles[0]
        print(browser.title)

        for row in browser.find_elements_by_id('ctl00_ContentPage_uxReportGrid_ctl00__0'):
            cell = row.find_elements_by_tag_name("td")[0]
            click_me = "//*[text()='{}']".format(cell.text)
            browser.find_element_by_xpath(click_me).click()

            #go to new window
            window_after = browser.window_handles[1]
            browser.switch_to.window(window_after)
            print(browser.title)
            time.sleep(5)

            #download chargeback
            browser.find_element_by_id("ctl00_ContentPage_uxExportChargebacks_litExport").click()
            browser.find_element_by_id("ctl00_ContentPage_uxExportChargebacks_imgCSV").click()

            #go back to previus page
            browser.close();
            browser.switch_to.window(window_before)

        browser.close();

if __name__ == '__main__':
    mn = Main()
    #getchargeback()
    mn.parse_csv()
