import time, sys, os, csv, shutil
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from decimal import Decimal, getcontext

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
card_dict = {
    'vi':'Visa',
    'ae':'Amex',
    'mc':'MasterCard',
    'dv':'Discover',
    'eb':'EBT',
    'db':'PinDebit'
}

def firstConn():
    """Connect to firstdata db"""
    firsty = create_engine(URL(**firstdata), strategy='threadlocal')
    firsty.echo = False
    return firsty

def to_type(x):
    y = x.lower()
    if y in card_dict:
        return card_dict[y]
    else:
        return 'Other'

def to_int(x):
    return int(x)

def to_cent(x):
    getcontext().prec = 6
    y = str(x)
    y = y.replace('$', '').replace(',', '')
    try:
        y = y.replace('(', '').replace(')', '')
    except:
        """Justing holding this up"""
    y = Decimal(y)
    return int(y*100)

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
            with open(my_file) as csvfile:
                readCSV = csv.reader(csvfile, delimiter=',')
                next(readCSV)
                for row in readCSV:
                    my_row = row
                    break
                mid = my_row[0].split(':')[1].strip()
                print(mid)
            if file.endswith(".csv"):
                print(my_file)
                my_panda = pd.read_csv(my_file, header=2, index_col='CB Sequence Number', parse_dates=True, infer_datetime_format=True)
                my_panda = my_panda.drop(my_panda.index[len(my_panda)-1])
                my_panda['mid'] = mid
                #my_panda.reset_index(level=0, inplace=True)
                my_panda.rename(columns={'Report Date': 'processing-date', 'Trans Date': 'transaction-date', 'Trans Date': 'transaction-date', 'File Source': 'file_source', 'Card Type': 'card-type', 'Cardholder Number': 'card-number', 'CB Type': 'tran-type', 'Reason Text': 'reason-desc', 'Disposition': 'disposition', 'Reference Number': 'reference-number', 'CB Sequence Number': 'ID','CB Reason Code':'record-type', 'Reason Text':'message','CB Amount':'amount','1st CB Amount':'1st_amount'}, inplace=True)
                my_panda= my_panda.fillna(0)
                my_panda['processing-date'] = pd.to_datetime(my_panda['processing-date']).astype('datetime64[ns]', copy=True)
                my_panda['transaction-date'] = pd.to_datetime(my_panda['transaction-date']).astype('datetime64[ns]', copy=True)
                my_panda['tran-type'] = my_panda['tran-type'].astype('int', copy=True).astype('str', copy=True)
                my_panda['record-type'] = my_panda['record-type'].astype('int', copy=True).astype('str', copy=True)
                my_panda['card-type'] = my_panda['card-type'].apply(to_type)
                my_panda['1st_amount'] = my_panda['1st_amount'].apply(to_cent)
                my_panda['amount'] = my_panda['amount'].apply(to_cent)
                my_date = str(my_panda.iloc[0]['processing-date']).replace('/','-')
                headers = '`,`'.join(list(my_panda))
                try:
                    my_panda.to_sql('chargebacks', self.confir, if_exists='append', index=False, chunksize=1000)
                except:
                    m = 0
                    n = 100
                    if n > int(my_panda.shape[0]):
                        n = int(my_panda.shape[0])
                    #print(my_panda.shape)
                    while n <= int(my_panda.shape[0]):
                        update_list = []
                        for j, item in my_panda[m:n].iterrows():
                            """Place in the update script"""
                            update = '","'.join(str(k) for k in item.values.tolist())
                            update_list.append('("{}")'.format(update))
                        #print(update_list)
                        insert_me = 'insert into chargebacks(`{}`) VALUES{}'.format(headers, ','.join(update_list))
                        #print(insert_me)
                        if not callConnection(self.confir, insert_me):
                            for j, item in my_panda[m:n].iterrows():
                                """Place in the update script"""
                                update = '","'.join(str(k) for k in item.values.tolist())
                                str_update = ','.join('`{}`="{}"'.format(key, item) for key, item in item.items())
                                insert_me = 'insert into chargebacks(`{}`) VALUES("{}") ON DUPLICATE KEY UPDATE {}'.format(headers, update, str_update)
                                #print(insert_me)
                                if not callConnection(self.confir, insert_me):
                                    print("Something went wronge")
                                    break

                        if n == my_panda.shape[0]:
                            break
                        elif n+100 > my_panda.shape[0]:
                            n = my_panda.shape[0]
                            m += 100
                        else:
                            n += 100
                            m += 100

                print(my_panda)
            shutil.move(my_file, 'done/'+str(my_date)+str(mid)+'.csv')

    def getchargeback(self):
        ###Setup options for chrome web browser
        options = webdriver.ChromeOptions()
        options.binary_location = '/usr/bin/google-chrome'
        prefs = {"download.default_directory" : dwn, "Page.setDownloadBehavior": {"behavior" : "allow", "downloadPath": dwn}}
        options.add_experimental_option('prefs', prefs)
        #options.add_argument('headless')
        options.add_argument('window-size=1200x600')
        browser = webdriver.Chrome(chrome_options=options)
        browser.get('https://www.youraccessone.com')

        username = browser.find_element_by_id("txtUserName")
        password = browser.find_element_by_id("txtPassword")

        username.send_keys(os.environ['username'])
        password.send_keys(os.environ['password'])

        browser.find_element_by_name("uxLogin").click()

        ###go to chargebacks
        browser.get('https://www.youraccessone.com/64_rpt_Chargebacks.aspx')

        ###open filter
        browser.find_element_by_xpath("//*[text()='FILTER']").click()

        ###toggle daily
        browser.find_element_by_xpath("//*[text()='Daily']").click()

        ###This section for date range not currently used change Daily to Date Range then uncomment###
        #get date range
        #start = browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxDateRangeFrom_dateInput")
        #start.clear()
        #start.send_keys('02-08-2016')

        ###check all merchants
        browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxOmahaHierarchies_uxchbAllMerchants").click()

        ###submit to apply filters
        browser.find_element_by_id("ctl00_ContentPage_uxFiltering_uxbtnDisplayRunReport").click()

        time.sleep(10)

        window_before = browser.window_handles[0]
        print(browser.title)
        #print(browser.page_source)

        while True:
            table_id = browser.find_element_by_class_name("rgMasterTable")
            for row in table_id.find_elements_by_class_name("rgRow"):
                print(row)
                cell = [item for item in row.find_elements_by_css_selector("td")][0]
                click_me = "//*[text()='{}']".format(cell.text)
                print(click_me)
                browser.find_element_by_xpath(click_me).click()

                #go to new window
                window_after = browser.window_handles[1]
                browser.switch_to.window(window_after)
                print(browser.title)
                time.sleep(5)

                #download chargeback
                element_to_hover_over = browser.find_element_by_xpath("//*[text()='EXPORT']")
                hover = ActionChains(browser).move_to_element(element_to_hover_over)
                hover.perform()

                browser.find_element_by_xpath("//*[text()='CSV']").click()

                #go back to previus page
                browser.close();
                print(browser.window_handles)
                browser.switch_to.window(browser.window_handles[0])
            try:
                browser.find_element_by_name('ctl00$ContentPage$uxReportGrid$ctl00$ctl03$ctl01$ctl00$pagerNextButton').click()
                time.sleep(5)
            except:
                break

        browser.close();

if __name__ == '__main__':
    arv = sys.argv
    print(arv)
    mn = Main()
    if '-r' in arv:
        mn.getchargeback()
    if '-p' in arv:
        mn.parse_csv()
    else:
        print("-r 'get charges' -p 'parse charges'")
