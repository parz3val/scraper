"""
Floorsheet scraper
"""
from typing import Tuple, Union, Any
from selenium import webdriver
import time
import os
import pandas as pd
from pandas import DataFrame
import csv
import threading
import gc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException as E

# from multiprocessing.pool import ThreadPool

# To do :
# Multiprocessing
# - Recursive Wait
# - Rotate proxies
# http://www.freeproxylists.net/
# Base URL for the company profile page
# http://merolagani.com/CompanyDetail.aspx?symbol=ADBL#0
# http://merolagani.com/CompanyDetail.aspx?symbol=NLG#0

BASE_URL = "http://merolagani.com/CompanyDetail.aspx?symbol="
IMP_DELAY = 5
TIMEOUT = 30
page_block_id = (
    "ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_litRecords"
)
DIV_ID = "ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet"
table_id = "/html/body/form/div[4]/div[6]/div/div/div/div[5]/div[2]/div[2]/table/tbody/tr[2]/td[1]"
# Add driver to path
os.environ["PATH"] += os.pathsep + r"driver"

# Initialize the driver object

# Load symbols
with open("ss.csv", "r") as symbols_file:
    symbols = [row["Symbol"] for row in csv.DictReader(symbols_file)]

threadLocal = threading.local()


def gets_driver():
    driver_a = getattr(threadLocal, "driver", None)
    if driver_a is None:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        driver_a = webdriver.Chrome(options=chrome_options)
        setattr(threadLocal, "driver", driver_a)
    return driver_a


def get_driver():
    return webdriver.Chrome()


def better_id_selector(browser_object, object_id, delay=TIMEOUT):
    try:
        element = WebDriverWait(browser_object, timeout=delay, poll_frequency=4).until(
            EC.element_to_be_clickable((By.ID, object_id))
        )
    except E:
        browser_object.quit()
        raise E
    return element


def better_x_path_selector(browser_object, object_xpath, delay=TIMEOUT):
    try:
        element = WebDriverWait(browser_object, timeout=delay, poll_frequency=4).until(
            EC.element_to_be_clickable((By.XPATH, object_xpath))
        )
    except E:
        browser_object.quit()
        raise E
    return element


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
        raise ReadException


def get_pages(browser_object, intent_block):
    """ Returns first and last pages """
    paging_span = better_id_selector(browser_object, page_block_id)
    page_string = str(paging_span.get_attribute("innerHTML"))
    return {
        "current_page": int(int(page_string.split(" ")[3]) / 100),
        "last_page": int(page_string.split(":")[-1].replace("]", "")),
    }


def is_table_loaded(browser_object, text, object_id=table_id):
    try:
        element = WebDriverWait(
            browser_object, timeout=TIMEOUT, poll_frequency=4
        ).until(EC.text_to_be_present_in_element((By.XPATH, object_id), text_=text))
    except E:
        browser_object.quit()
        raise E
    return element


def symbol_to_csv(symbol: str, browser_object: webdriver):
    """ Takes in the symbol and returns floorsheet dataframe """
    search_url: str = f"{BASE_URL}{symbol}"
    browser_object.get(search_url)
    # Click on the nav button
    elm = browser_object.find_element_by_id("navFloorSheet")
    browser_object.implicitly_wait(2)
    #print(elm.get_attribute("innerHTML"))
    elm.submit()
    # Wait until data is loaded
    is_table_loaded(browser_object, "1")
    intent_block = browser_object.find_element_by_id(DIV_ID)
    frame_block = block_to_frame(intent_block)
    frames_to_csv(symbol, frame_block)
    print(f"For the symbol {symbol} and page: {1}")
    # better_x_path_selector(browser_object, "//a[@title='Next Page']")
    browser_object.implicitly_wait(IMP_DELAY)
    # next_button.click()
    i = 1
    j = 1
    last_page = get_pages(browser_object, intent_block)["last_page"]
    for _ in range(last_page-1):
        try:
            next_button = better_x_path_selector(
                browser_object, "//a[@title='Next Page']"
            )
            next_button.click()
        except Exception as e:
            print(str(e))

        print(
            f'Current Page: {get_pages(browser_object, intent_block)["current_page"]}'
        )
        print(f'Current Page: {get_pages(browser_object, intent_block)["last_page"]}')
        i = i + 100
        j = j + 1
        print(f"For the symbol {symbol} and page: {j}")
        is_table_loaded(browser_object, str(i))
        intent_block = browser_object.find_element_by_id(DIV_ID)
        inter_frame = block_to_frame(intent_block)
        frames_to_csv(symbol, inter_frame, no_index=True)

        # For running in short steps
    print("Aloha!")


def frames_to_csv(symbol, frame, no_index=False):
    if no_index:
        with open(f"sheets/{symbol}.csv", "a+") as file:
            frame.to_csv(file, index=False, header=False)
    with open(f"sheets/{symbol}.csv", "a+") as file:
        frame.to_csv(file, index=False)


"""
for sym in symbols:
    driver = get_driver()
    ThreadPool(1).map(frames_to_csv, symbol_to_frames(sym, get_driver()))
"""

driver = gets_driver()


for sym in symbols:
    # symbol shorts
    symbol_to_csv(sym, driver)
    print("Reached here!")
driver.quit()

gc.collect()
