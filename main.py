"""
Floorsheet scraper
"""
from selenium import webdriver
import time
import os
import pandas as pd
from pandas import DataFrame
import csv
import threading
import multiprocessing
from multiprocessing import freeze_support
from datetime import datetime

# selenium imports
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException as E

# Server for network information
from browsermobproxy import Server
server = Server("browsermob-proxy/bin/browsermob-proxy.bat")
server.start()
proxy = server.create_proxy()



# To do :
# - Recursive Wait
# - Rotate proxies
# http://www.freeproxylists.net/
# Base URL for the company profile page
# http://merolagani.com/CompanyDetail.aspx?symbol=ADBL#0
# http://merolagani.com/CompanyDetail.aspx?symbol=NLG#0
IMP_DELAY  = 10
TIMEOUT = 15
DIV_ID = "ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet"
BASE_URL = "http://merolagani.com/CompanyDetail.aspx?symbol="
page_block_id ="ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_litRecords"
table_id = "/html/body/form/div[4]/div[6]/div/div/div/div[5]/div[2]/div[2]/table/tbody/tr[2]/td[1]"

# Add driver to path
os.environ["PATH"] += os.pathsep + r'driver'

# Initialize the driver object
# driver = webdriver.Chrome()

# Load symbols
with open('sr.csv', "r") as symbols_file:
    symbols = [row["Symbol"] for row in csv.DictReader(symbols_file)]

threadLocal = threading.local()



def get_driver():
    a_driver = getattr(threadLocal, 'driver', None)
    if a_driver is None:
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--proxy-server={0}".format(proxy.proxy))
        a_driver = webdriver.Chrome(executable_path='chromedriver.exe',options=chrome_options)
        
        setattr(threadLocal, 'driver', a_driver)
    return a_driver



def block_to_frame(intent_block) -> DataFrame:
    """
    Takes in block of code
    Finds the tables in the block
    :returns : first table frame
    """
    html_table = intent_block.find_element_by_tag_name("table")
    try:
        return pd.read_html(str(html_table.get_attribute("outerHTML")))[0]
    except Exception as ReadException:
        print(ReadException)


def wait(DELAY=6):
    """ Explicit wait """
    time.sleep(DELAY)


def better_id_selector(browser_object, object_id, delay=TIMEOUT):
    try:
        element = WebDriverWait(browser_object, timeout=delay, poll_frequency=4).until(
            EC.element_to_be_clickable((By.ID, object_id))
        )
    except E:
        browser_object.quit()
        raise E
    return element

    
def get_index(intent_block):
    current_page_no = intent_block.find_element_by_class_name("current_page")
    return str(current_page_no.get_attribute('innerHTML'))

def has_next_page(browser_object):
    try:
        return browser_object.find_element_by_xpath("//a[@title='Next Page']")
    except:
        return False

def frames_to_csv(symbol, frame, no_index=False):
    # if no_index:
    #     with open(f'sheets/{symbol}.csv', 'a+',newline='') as file:
    #         frame.to_csv(file, index=False, header=False)
    with open(f'sheets/{symbol}.csv', 'a+',newline='') as file:
        frame.to_csv(file, index=False)

def get_pages(browser_object, intent_block):
    """ Returns first and last pages """
    paging_span = better_id_selector(browser_object, page_block_id)
    page_string = str(paging_span.get_attribute("innerHTML"))
    return {
        "current_page": int(int(page_string.split(" ")[3]) / 100),
        "last_page": int(page_string.split(":")[-1].replace("]", "")),
    }


def reload_and_continue(browser,data):
    index:int = int(int(data)/100)+1
    # Reload the page
    browser.refresh()
    # get the nav_btn
    nav_btn = better_id_selector(browser, 'navFloorSheet', delay=TIMEOUT)
    # Click on the nav button
    nav_btn.click()
    # wait for the nav page to load
    wait()
    # Continue from the given page
    index_link = f"changePageIndex('{index}','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_hdnCurrentPage','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_btnPaging')"
    browser.execute_script(index_link)
    return index

def wait_until_load(browser_object, text, object_id=table_id):
    try:
        element = WebDriverWait(
            browser_object, timeout=TIMEOUT, poll_frequency=3
        ).until(EC.text_to_be_present_in_element((By.XPATH, object_id), text_=text))
    except E:
        # browser_object.quit()
        reload_and_continue(browser_object,text)
        wait_until_load(browser_object, text, object_id=table_id)
        raise E
    return element

def symbol_to_csv(symbol: str, browser_object: webdriver):
    """ Takes in the symbol and returns floorsheet dataframe """

    search_url: str = f"{BASE_URL}{symbol}"
    browser_object.get(search_url)
    nav_btn = better_id_selector(browser_object, 'navFloorSheet', delay=TIMEOUT)
    # Click on the nav button
    nav_btn.click()

    # waiting for table to load with data
    wait_until_load(browser_object, "1")
    intent_block = browser_object.find_element_by_id(DIV_ID)
    frame_block = block_to_frame(intent_block)

    # Frames to csv data
    frames_to_csv(symbol, frame_block)

    # Will get this dynamically later
    last_page_no = get_pages(browser_object, intent_block)["last_page"]

    i:int = 1
    j:int = 1
    while i < last_page_no:
        
        # Increment the counter
        i = i + 1
        j = j + 100

        try:
            next_button = browser_object.find_element_by_xpath("//a[@title='Next Page']")
            next_button.click()
            next_button.click()
        except Exception as e:
            print(str(e))
            intent_block.find_element_by_class_name("current_page").click()
            time.sleep(6)
            next_button.click()
        

        #Logic here
        print(i,get_index(browser_object))
        print(f"For the symbol {symbol} and page: {i}")

        wait_until_load(browser_object, str(j))
        intent_block = browser_object.find_element_by_id(DIV_ID)
        inter_frame = block_to_frame(intent_block)
        frames_to_csv(symbol, inter_frame, no_index=True)
        


if __name__ == "__main__":
    # runner()

    symbol='NICAD8283'
    window = get_driver() 
    # symbol_to_csv(symbol,window)

    for symbol in symbols:
        try:
            symbol_to_csv(symbol,window)
        except:
            log_file = open("log.txt",'a+', encoding = 'utf-8')
            now = datetime.now()
            time_now =  str(now.strftime("%m/%d/%Y, %H:%M:%S"))
            log_file.write(f"[ {time_now} ] Error downloading {symbol}.\n")
    window.quit()