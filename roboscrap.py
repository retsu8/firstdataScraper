import time, sys
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup

profile = webdriver.FirefoxProfile()
profile.set_preference("browser.download.folderList", 2)
profile.set_preference("browser.download.manager.showWhenStarting", False)
profile.set_preference("browser.download.dir", '/home/wjp/Documents/git/firstdataScraper/csv/')
profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

browser = webdriver.Firefox(firefox_profile=profile)
browser.get('https://www.youraccessone.com')

username = browser.find_element_by_id("txtUserName")
password = browser.find_element_by_id("txtPassword")

username.send_keys("paddockw")
password.send_keys("43#UjmS4RonG$O3&T3KL")

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
