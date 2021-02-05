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
import rollbar



# To do :
# - Recursive Wait
# - Rotate proxies
# http://www.freeproxylists.net/
# Base URL for the company profile page
# http://merolagani.com/CompanyDetail.aspx?symbol=ADBL#0
# http://merolagani.com/CompanyDetail.aspx?symbol=NLG#0

BASE_URL = "http://merolagani.com/CompanyDetail.aspx?symbol="
IMP_DELAY = 10

# Add driver to path
os.environ["PATH"] += os.pathsep + r'driver'

# Initialize the driver object
# driver = webdriver.Chrome()

# Load symbols
with open('ss.csv', "r") as symbols_file:
    symbols = [row["Symbol"] for row in csv.DictReader(symbols_file)]

threadLocal = threading.local()
rollbar.init('d80aab892f46407f9018bac76bf108a7')
rollbar.report_message('Rollbar is configured correctly')


def get_driver():
    a_driver = getattr(threadLocal, 'driver', None)
    if a_driver is None:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        a_driver = webdriver.Chrome(options=chrome_options)
        setattr(threadLocal, 'driver', a_driver)
    return a_driver


def block_to_frame(intent_block) -> DataFrame:
    """
    Takes in block of code
    Finds the tables in the block
    :returns : first table frame
    """
    try:
        html_table = intent_block.find_element_by_tag_name("table")
        return pd.read_html(str(html_table.get_attribute("outerHTML")))[0]
    except Exception as e:
        rollbar.report_message(str(e))


def wait():
    """ Explicit wait """
    time.sleep(IMP_DELAY)


def get_pages(intent_block):
    """ Returns first and last pages """
    paging_span = intent_block.find_element_by_id("ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_litRecords")
    page_string = str(paging_span.get_attribute("innerHTML"))
    return {
        "current_page": int(page_string.split(" ")[3])/100,
        "last_page": int(page_string.split(":")[-1].replace("]", ""))
    }


def symbol_to_frames(symbol: str, browser_object: webdriver) -> DataFrame:
    """ Takes in the symbol and returns floorsheet dataframe """
    search_url: str = f"{BASE_URL}{symbol}"
    browser_object.get(search_url)
    browser_object.implicitly_wait(IMP_DELAY)
    # Click on the nav button
    browser_object.find_element_by_id("navFloorSheet").click()
    wait()
    intent_block = browser_object.find_element_by_id("ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet")
    frame_block = block_to_frame(intent_block)
    next_button = browser_object.find_element_by_xpath("//a[@title='Next Page']")
    while get_pages(intent_block)["current_page"] <= get_pages(intent_block)["last_page"]:
        next_button.click()
        wait()
        next_button = browser_object.find_element_by_xpath("//a[@title='Next Page']")
        intent_block = browser_object.find_element_by_id("ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet")
        inter_frame = block_to_frame(intent_block)
        frame_block = pd.concat([frame_block, inter_frame], axis=0)
    return frame_block


def symbol_to_csv(shortcode):
    window = get_driver()
    floor_data = symbol_to_frames(shortcode, window)
    with open(f'sheets/{shortcode}.csv', 'w') as file:
        floor_data.to_csv(file)
    window.quit()


def runner():
    for i in range(len(symbols)):
        ctr_flag = 1
        processes = []
        sym = symbols[i]
        p = multiprocessing.Process(target=symbol_to_csv, args=[sym])
        p.start()
        processes.append(p)
        ctr_flag += 1
        if i > 5:
            for process in processes:
                process.join()
            ctr_flag = 1
            processes = []


if __name__ == "__main__":
    runner()
