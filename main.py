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


# To do :
# - Recursive Wait
# - Rotate proxies
# http://www.freeproxylists.net/
# Base URL for the company profile page
# http://merolagani.com/CompanyDetail.aspx?symbol=ADBL#0
# http://merolagani.com/CompanyDetail.aspx?symbol=NLG#0
IMP_DELAY = 6
TIMEOUT = 60
DIV_ID = "ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet"
BASE_URL = "http://merolagani.com/CompanyDetail.aspx?symbol="
page_block_id = "ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_litRecords"
table_id = "/html/body/form/div[4]/div[6]/div/div/div/div[5]/div[2]/div[2]/table/tbody/tr[2]/td[1]"

# Add driver to path
os.environ["PATH"] += os.pathsep + r"driver"

# Initialize the driver object
# driver = webdriver.Chrome()

# Load symbols
with open("ss.csv", "r") as symbols_file:
    symbols = [row["Symbol"] for row in csv.DictReader(symbols_file)]

threadLocal = threading.local()


def get_driver():
    a_driver = getattr(threadLocal, "driver", None)
    if a_driver is None:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        a_driver = webdriver.Chrome(options=chrome_options)

        setattr(threadLocal, "driver", a_driver)
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


def wait(DELAY=IMP_DELAY):
    """ Explicit wait """
    time.sleep(DELAY)


def better_id_selector(browser_object, object_id, delay=TIMEOUT):
    try:
        element = WebDriverWait(
            browser_object, timeout=delay, poll_frequency=4
        ).until(EC.element_to_be_clickable((By.ID, object_id)))
    except E:
        browser_object.quit()
        raise E
    return element


def get_index(intent_block):
    current_page_no = intent_block.find_element_by_class_name("current_page")
    return str(current_page_no.get_attribute("innerHTML"))


def has_next_page(browser_object):
    try:
        return browser_object.find_element_by_xpath("//a[@title='Next Page']")
    except Exception as e:
        return False


def frames_to_csv(symbol, frame, no_index=False):
    if no_index:
        with open(f"sheets/{symbol}.csv", "a+", newline="") as file:
            frame.to_csv(file, index=False, header=False)
    with open(f"sheets/{symbol}.csv", "a+", newline="") as file:
        frame.to_csv(file, index=False)


def get_pages(browser_object, intent_block):
    """ Returns first and last pages """
    paging_span = better_id_selector(browser_object, page_block_id)
    page_string = str(paging_span.get_attribute("innerHTML"))
    return {
        "current_page": int(int(page_string.split(" ")[3]) / 100),
        "last_page": int(page_string.split(":")[-1].replace("]", "")),
    }


def reload_and_continue(browser, data):
    index: int = int(int(data) / 100) + 1
    # Reload the page
    browser.refresh()
    # get the nav_btn
    nav_btn = better_id_selector(browser, "navFloorSheet", delay=TIMEOUT)
    # Click on the nav button
    nav_btn.click()
    # wait for the nav page to load
    wait()
    # Continue from the given page
    index_link = f"changePageIndex('{index}','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_hdnCurrentPage','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_btnPaging')"
    browser.execute_script(index_link)
    wait()
    return 1


def wait_until_table_is_loaded(browser_object, text, object_id=table_id):
    try:
        element = WebDriverWait(
            browser_object, timeout=TIMEOUT, poll_frequency=3
        ).until(
            EC.text_to_be_present_in_element((By.XPATH, object_id), text_=text)
        )
    except E:
        # browser_object.quit()
        element = reload_and_continue(browser_object, text)
        # raise E
    return element


def symbol_to_csv(symbol: str, browser_object: webdriver = get_driver()):
    """ Takes in the symbol and returns floorsheet dataframe """
    search_url: str = f"{BASE_URL}{symbol}"
    browser_object.get(search_url)
    nav_btn = better_id_selector(browser_object, "navFloorSheet", delay=TIMEOUT)
    # Click on the nav button
    nav_btn.click()

    # waiting for table to load with data
    wait_until_table_is_loaded(browser_object, "1")
    intent_block = browser_object.find_element_by_id(DIV_ID)
    frame_block = block_to_frame(intent_block)

    # Frames to csv data
    frames_to_csv(symbol, frame_block)

    # Will get this dynamically later
    last_page_no = get_pages(browser_object, intent_block)["last_page"]

    i: int = 1
    j: int = 1
    while i < last_page_no:

        # Increment the counter
        i = i + 1
        j = j + 100

        index_link = f"changePageIndex('{i}','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_hdnCurrentPage','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_btnPaging')"
        browser_object.execute_script(index_link)

        # Logic here
        print(i, get_index(browser_object))
        print(f"For the symbol {symbol} and page: {i}")

        wait_until_table_is_loaded(browser_object, str(j))
        intent_block = browser_object.find_element_by_id(DIV_ID)
        inter_frame = block_to_frame(intent_block)
        frames_to_csv(symbol, inter_frame, no_index=True)


def runner():
    window = get_driver()
    # symbol_to_csv(symbol,window)

    for i in range(len(symbols)):
        ctr_flag = 1
        num_processes = 10
        processes = []
        sym = symbols[i]
        p = multiprocessing.Process(target=symbol_to_csv, args=[sym])
        p.start()
        processes.append(p)
        ctr_flag += 1
        # How many threads/processes you want to run
        if i > num_processes:
            for process in processes:
                process.join()
            ctr_flag = 1
            processes = []

    window.quit()


if __name__ == "__main__":
    runner()
