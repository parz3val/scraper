"""
Floorsheet scraper
"""
from selenium import webdriver
import time
import os
import pandas as pd
from pandas import DataFrame
import csv


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
driver = webdriver.Chrome()

# Load symbols
with open('ss.csv', "r") as symbols_file:
    symbols = [row["Symbol"] for row in csv.DictReader(symbols_file)]


def block_to_frame(intent_block) -> DataFrame:
    """
    Takes in block of code
    Finds the tables in the block
    :returns : first table frame
    """
    html_table = intent_block.find_element_by_tag_name("table")
    return pd.read_html(str(html_table.get_attribute("outerHTML")))[0]


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
    next_button = browser_object.find_element_by_xpath("//a[@title='Next Page']")
    frame_block = block_to_frame(intent_block)
    i = 0
    while get_pages(intent_block)["current_page"] <= get_pages(intent_block)["last_page"]:
        i += 1
        print(i)
        next_button.click()
        wait()
        next_button = browser_object.find_element_by_xpath("//a[@title='Next Page']")
        intent_block = browser_object.find_element_by_id("ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet")
        inter_frame = block_to_frame(intent_block)
        frame_block = pd.concat([frame_block, inter_frame], axis=0)

        # For running in short steps
        if i > 4:
            break
    return frame_block


j = 0
for sym in symbols:
    # symbol shorts
    j += 1
    if j > 2:
        break
    df = symbol_to_frames(sym, driver)
    with open(f'{sym}.csv', 'w') as f:
        df.to_csv(f)

driver.quit()
