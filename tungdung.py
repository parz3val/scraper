"""
Floorsheet scraper
"""
import time
import os
import pandas as pd
from pandas import DataFrame
import csv

# selenium imports
from selenium import webdriver
from multiprocessing import freeze_support
import threading
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException as E

IMP_DELAY:int = 10
TIMEOUT:int = 12
DIV_ID = "ctl00_ContentPlaceHolder1_CompanyDetail1_divDataFloorsheet"
page_block_id = ("ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_litRecords")
table_id = "/html/body/form/div[4]/div[6]/div/div/div/div[5]/div[2]/div[2]/table/tbody/tr[2]/td[1]"
BASE_URL = "http://merolagani.com/CompanyDetail.aspx?symbol="
# load threads
threadLocal = threading.local()

# Get the Driver
def get_driver(headless=False):
    """ 
    Get the Chrome Driver
    If headless = True then 
    Without GUI"""
    a_driver = getattr(threadLocal, 'driver', None)
    if a_driver is None:
        chrome_options = webdriver.ChromeOptions()
        if(headless):
            chrome_options.add_argument("--headless")
        a_driver = webdriver.Chrome(options=chrome_options)
        setattr(threadLocal, 'driver', a_driver)
    return a_driver


class Tungdung_serial():
  # when the class is called perform these functions
    def __init__(self,symbol,window):
        #Input Variables for this Tundung
        self.symbol=symbol
        self.window = window
        # Used for current data status
        self.current_index: int = 1
        self.table_top_data: int = 1
        # start the main function
        # self.symbol_to_csv(self.symbol)

    def block_to_frame(self,intent_block) -> DataFrame:
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

    def wait(self, DELAY=6):
        """ Explicit wait """
        time.sleep(DELAY)

    def better_id_selector(self, object_id):
        try:
            element = WebDriverWait(self.window, timeout=TIMEOUT, poll_frequency=3).until(
                EC.element_to_be_clickable((By.ID, object_id))
            )
        except Exception as E:
            self.logger_error(str(E))

    def get_index(self):
        intent_block = self.window.find_element_by_id(DIV_ID)
        current_page_no = intent_block.find_element_by_class_name("current_page")
        return str(current_page_no.get_attribute("innerHTML"))

    def drop_and_continue(self):
        """ Get new Browser and continue """
        self.window.quit()


    def reload_and_continue(self):
        # Reload the page
        self.window.refresh()
        # Wait 2 seconds for the page to load
        self.wait()
        # get the nav_btn and Click on the nav button
        nav_btn = self.better_id_selector("navFloorSheet")
        # wait for the nav page to load
        nav_btn.click()
        self.wait()
        # Continue from the given page
        index_link = f"changePageIndex('{self.current_index}','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_hdnCurrentPage','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_btnPaging')"
        self.window.execute_script(index_link)
        self.wait()


    def wait_until_load(self,object_id=table_id):
        """ Waits until the element is loaded with data """
        try:
            element = WebDriverWait(
                self.window, timeout=TIMEOUT, poll_frequency=3
            ).until(EC.text_to_be_present_in_element((By.XPATH, object_id), text_=str(self.table_top_data)))                
        except E:
            self.logger_error(str(E))
            print('TimeOut')
            self.reload_and_continue()

    def frames_to_csv(self,symbol,no_index=False):
        """ 
        Gets the table data as frame 
        Saves the Data as CSV
        """
        # Wait until page loads
        self.wait_until_load()
        # get the frames
        try:
            intent_block = self.window.find_element_by_id(DIV_ID)
            frame_block = self.block_to_frame(intent_block)
            # Write the CSV File
            with open(f"sheets/{symbol}.csv", "a+", newline="") as file:
                # for error None type has no property to_csv
                frame_block.to_csv(file, index=False)
        except:
            self.logger_failure()

    def get_pages(self):
        """ Returns first and last pages """
        paging_span = self.better_id_selector(page_block_id)
        page_string = str(paging_span.get_attribute("innerHTML"))
        return {
            "current_page": int(int(page_string.split(" ")[3]) / 100),
            "last_page": int(page_string.split(":")[-1].replace("]", "")),
        }
    
    def logger_failure(self):
        with open(f"sheets/{symbol}_falied.log", "a+", newline="") as file:
            # for error None type has no property to_csv
            file.write(f"{self.current_index},{self.symbol}\n")

    def logger_error(self,Err):
        with open(f"sheets/error.log", "a+", newline="") as file:
            file.write(f"{self.symbol},{self.current_index} : {Err}\n")