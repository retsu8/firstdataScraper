import time, sys, os, csv, shutil, datetime, string
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
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
    'database': 'druporta_tss_data',
}

card_dict = {
    'vi':'Visa',
    'ae':'Amex',
    'mc':'MasterCard',
    'dv':'Discover',
    'eb':'EBT',
    'db':'PinDebit'
}

country_codes = {}
country = os.path.join(direct, 'countryCodes.csv')
with open(country) as csvfile:
    reader = csv.reader(csvfile)
    country_codes = {rows[0].lower():rows[1] for rows in reader}

def generate(mini=170, maxi=250):
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
    size = randint(mini, maxi)
    return ''.join(choice(chars) for _ in range(size))

def firstConn():
    """Connect to firstdata db"""
    firsty = create_engine(URL(**firstdata), strategy='threadlocal')
    firsty.echo = False
    return firsty

def to_int(x):
    return int(x)

def to_cent(x):
    getcontext().prec = 6
    try:
        y = x.replace('(', '').replace(')', '')
    except:
        """Justing holding this up"""
    y = Decimal(y)
    return int(y*100)

def to_type(x):
    y = x.lower()
    if y in card_dict:
        return card_dict[y]
    else:
        return 'Other'

def to_countrycode(x):
    x = x.lower()
    if x in country_codes:
        return country_codes[x]

def make_type_nice(x):
    if 'R' == x or 'RTRN' == x:
        return str('Refund')
    else:
        return str('Sale')

def build_tran_id(my_panda):
    return str(my_panda['reference-number']) + " "+ str(my_panda['File Source']) +' '+ str(generate(10, 15))

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
        log.warn(sql)
        log.warn(sys.exc_info())
        inst = sys.exc_info()
        trans.rollback()  # this rolls back the transaction unconditionally
        raise
    return inst

class Main(object):

    @manage_firstdata
    def parse_csv(self):
        """Parse the csv pulled from the transactions table"""
        for file in os.listdir("csv"):
            my_file = os.path.join(dwn, file)
            if file.endswith(".csv"):
                print(my_file)
                my_panda = pd.read_csv(my_file, header=1, parse_dates=[4,5], infer_datetime_format=True, encoding='utf-8')
                #my_panda.reset_index(level=0, inplace=True)
                del my_panda['Matched']
                del my_panda['Keyed']
                del my_panda['Exp Date']
                my_panda.rename(columns={'Merchant #':'MID','Merchant Name':'merchant-name','Report Date': 'processing-date', 'Trans Date': 'transaction-date', 'Batch #':'batch-number', 'Trans Time':'transaction-time','Trans Code':'tran-type', 'Country Code':'country-code','Card Type':'card-type','Card #':'card-number','Auth #':'auth-code','Trans Amount':'amount','Terminal #':'reference-number'}, inplace=True)
                my_panda= my_panda.fillna(0)
                my_date = str(my_panda.iloc[0]['processing-date']).replace('/','-')
                my_panda['card-type'] = my_panda['card-type'].apply(to_type)
                my_panda['country-code'] = my_panda['country-code'].apply(to_countrycode)
                my_panda['tran-type'] = my_panda['tran-type'].apply(make_type_nice)
                my_panda['processing-date'] = pd.to_datetime(my_panda['processing-date']).astype('datetime64[ns]', copy=True)
                my_panda['transaction-date'] = pd.to_datetime(my_panda['transaction-date']).astype('datetime64[ns]', copy=True)
                my_panda['batch-date'] = pd.to_datetime(my_panda['transaction-date'])
                my_panda['tran-identifier'] = my_panda.apply(build_tran_id, axis=1)
                del my_panda['File Source']
                my_panda['amount'] = my_panda['amount'].replace('[\$,]', '', regex=True).apply(to_cent)
                my_panda.sort('processing-date', inplace=True)

                my_panda.to_csv(str(my_file)+'_panda.csv')
                headers = '`,`'.join(list(my_panda))
                sql = "LOAD DATA LOCAL INFILE '{}' REPLACE INTO TABLE druporta_tss_data.transactions FIELDS TERMINATED BY ',' lines terminated by '\n' IGNORE 1 LINES (`{}`);".format(str(my_file)+'_panda.csv', headers)
                if not callConnection(self.conn, sql):
                    try:
                        my_panda.to_sql('transactions', self.confir, if_exists='append', index=False, chunksize=100)
                    except:
                        for j, item in my_panda.iterrows():
                            """Place in the update script"""
                            update = '","'.join(str(k) for k in item.values.tolist())
                            del item['tran-identifier']
                            str_update = ','.join('`{}`="{}"'.format(key, item) for key, item in item.items())

                            insert_me = 'insert into transactions(`{}`) VALUES("{}") ON DUPLICATE KEY UPDATE {}'.format(headers, update, str_update)

                            #print(insert_me)

                            if not callConnection(self.confir, insert_me):
                                break
                else:
                    print('The load doata localfile executed properly and is now ready to use')

                print(my_panda)
            shutil.remove(str(my_file)+'_panda.csv')
            shutil.move(my_file, 'done/'+str(my_date)+'.csv')

    def gettransactions(self):
        """Get a csv from youraccessone transactions"""
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
