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
#from multiprocessing.pool import ThreadPool

# To do :
# Multiprocessing
# - Recursive Wait
# - Rotate proxies
# http://www.freeproxylists.net/
# Base URL for the company profile page
# http://merolagani.com/CompanyDetail.aspx?symbol=ADBL#0
# http://merolagani.com/CompanyDetail.aspx?symbol=NLG#0

BASE_URL = "http://merolagani.com/CompanyDetail.aspx?symbol="
IMP_DELAY = 20
page_block_id = "ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_litRecords"
table_block_id = "ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet"
# Add driver to path
os.environ["PATH"] += os.pathsep + r'driver'

# Initialize the driver object

# Load symbols
with open('ss.csv', "r") as symbols_file:
    symbols = [row["Symbol"] for row in csv.DictReader(symbols_file)]

threadLocal = threading.local()


def gets_driver():
    driver_a = getattr(threadLocal, 'driver', None)
    if driver_a is None:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        driver_a = webdriver.Chrome(options=chrome_options)
        setattr(threadLocal, 'driver', driver_a)
    return driver_a


def get_driver():
    return webdriver.Chrome()


def better_id_selector(browser_object, object_id, delay=IMP_DELAY):
    try:
        element = WebDriverWait(browser_object, timeout=delay, poll_frequency=4).until(
            EC.element_to_be_clickable((By.ID, object_id)))
    except E:
        browser_object.quit()
        raise E
    return element


def better_x_path_selector(browser_object, object_xpath, delay=IMP_DELAY):
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
    print(html_table.get_attribute("innerHTML"))
    try:
        return pd.read_html(str(html_table.get_attribute("outerHTML")))[0]
    except Exception as ReadException:
        print(ReadException)
        raise ReadException


def get_pages(intent_block):
    """ Returns first and last pages """
    paging_span = intent_block.find_element_by_id(page_block_id)
    page_string = str(paging_span.get_attribute("innerHTML"))
    return {
        "current_page": int(page_string.split(" ")[3])/100,
        "last_page": int(page_string.split(":")[-1].replace("]", ""))
    }


def is_loaded(browser_object):
    if better_x_path_selector(browser_object, "//a[@title='Next Page'']"):
        print("Loaded")


def symbol_to_frames(symbol: str, browser_object: webdriver):
    """ Takes in the symbol and returns floorsheet dataframe """
    search_url: str = f"{BASE_URL}{symbol}"
    browser_object.get(search_url)
    better_id_selector(browser_object, "navFloorSheet").click()
    intent_block = better_id_selector(browser_object, table_block_id)
    frame_block = block_to_frame(intent_block)
    next_button = better_x_path_selector(browser_object, "//a[@title='Next Page']")
    i = 0
    while get_pages(intent_block)["current_page"] <= get_pages(intent_block)["last_page"]:
        i = i + 1
        if i > 10:
            return frame_block
        if get_pages(intent_block)["current_page"] != get_pages(intent_block)["last_page"]:
            next_button.click()
            next_button = better_x_path_selector(browser_object, "//a[@title='Next Page']")
        intent_block = better_x_path_selector(browser_object, table_block_id)
        inter_frame = block_to_frame(intent_block)
        frame_block = pd.concat([frame_block, inter_frame], axis=0)
        # For running in short steps
    return frame_block


def frames_to_csv(symbol, frame):
    with open(f'{symbol}.csv', 'w') as file:
        frame.to_csv(file)


"""
for sym in symbols:
    driver = get_driver()
    ThreadPool(1).map(frames_to_csv, symbol_to_frames(sym, get_driver()))
"""


for sym in symbols:
    # symbol shorts
    driver = get_driver()
    floor_frames = symbol_to_frames(sym, driver)
    frames_to_csv(sym, floor_frames)
    driver.quit()

gc.collect()
