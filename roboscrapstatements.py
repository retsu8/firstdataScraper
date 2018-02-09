import time, sys, os, csv, shutil, re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from decimal import Decimal, getcontext
from pdf2txt import main as pd2t

direct = os.getcwd()
dwn = os.path.join(direct, 'pdf')

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

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

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

def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext

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
        for file in os.listdir("pdf"):
            my_file = os.path.join(dwn, file)
            print(my_file)
            pd2t([my_file, '-t', 'html', '-o', 'temp.html'])
            my_file = [item for item in open('temp.html', "r")]
            my_statement_dict = {}
            cleanup = []
            for line in my_file:
                cleanup.append(cleanhtml(line.strip()).lower())
            my_file = cleanup
            my_file = list(filter(None, my_file))
            #[print(item) for item in my_file]
            for line in my_file:
                if "merchant number" in line and 'mid' not in my_statement_dict:
                    merch_number = my_file.index(line)
                    my_statement_dict['mid'] = str(my_file[merch_number+4].replace(' ', ''))

                if "amounts submitted" in line and 'amount_submitted' not in my_statement_dict:
                    amount_submitted = my_file.index(line)
                    my_statement_dict['amount_submitted'] = str(my_file[amount_submitted+6])

                if "third party transactions" in line and 'third_party_transactions' not in my_statement_dict:
                    third_party_transactions = my_file.index(line)
                    my_statement_dict['third_party_transactions'] = str(my_file[amount_submitted+6])

                if "adjustments/chargebacks" in line and 'adjustments_chargebacks' not in my_statement_dict:
                    adjustments_chargebacks = my_file.index(line)
                    my_statement_dict['adjustments_chargebacks'] = str(my_file[amount_submitted+6])

                if "fees charged" in line and 'fees_charged' not in my_statement_dict:
                    fees_charged = my_file.index(line)
                    my_statement_dict['fees_charged'] = str(my_file[amount_submitted+6])

                if "summary by card type" in line and 'summary_card_type' not in my_statement_dict:
                    summary_card_type = my_file.index(line)
                    my_statement_dict['summary_card_type'] = ''

                if "amounts funded by batch" in line and 'batch_fund' not in my_statement_dict:
                    batch_fund = my_file.index(line)
                    my_statement_dict['batch_fund'] = ''

            sum_by_card = my_file[summary_card_type+6:batch_fund]
            ticket_loc = sum_by_card.index('items')
            sum_by_card = sum_by_card[:ticket_loc] + [0] + sum_by_card[ticket_loc:]
            sum_by_card = [x for x in sum_by_card if x != 'amount' and x != 'ticket' and x != 'average' and x != 'items']
            six_by = [item for item in chunks(sum_by_card, 6)]
            headers = six_by.pop(0)
            my_panda = pd.DataFrame(six_by, columns=headers, index=['avg_ticket','gross_sale_items','gross_sale_amt', 'refund_items', 'refund_amount', 'total_amount'])
            print(my_panda)

            #print(my_statement_dict)

            #print(my_file)

            sys.exit()
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
            shutil.move(my_file, 'done/'+str(my_date)+str(mid)+'.pdf')

    def getstatement(self):
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
        browser.get('https://www.youraccessone.com/64_rpt_Statements.aspx')

        time.sleep(5)

        while True:
            table_id = browser.find_element_by_class_name("rgMasterTable")
            for row in table_id.find_elements_by_class_name("rgRow"):
                print(row)
                cell = [item for item in row.find_elements_by_css_selector("td")][6]
                click_me = "//*[text()='{}']".format(cell.text)
                print(click_me)
                browser.find_element_by_xpath(click_me).click()

            try:
                browser.find_element_by_name('ctl00$ContentPage$uxReportGrid$ctl00$ctl03$ctl01$ctl00$pagerNextButton').click()
            except:
                break
            time.sleep(5)

        browser.close();

if __name__ == '__main__':
    arv = sys.argv
    print(arv)
    mn = Main()
    if '-r' in arv:
        mn.getstatement()
    if '-p' in arv:
        mn.parse_csv()
    else:
        print("-r 'get charges' -p 'parse charges'")
