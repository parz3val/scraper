import os
import multiprocessing
import csv
from datetime import datetime
from tungdung import Tungdung_serial,get_driver

# Base URL of the website
BASE_URL = "http://merolagani.com/CompanyDetail.aspx?symbol="

# Load symbols
with open("sr.csv", "r") as symbols_file:
    symbols = [row["Symbol"] for row in csv.DictReader(symbols_file)]

# Kept outside the class to support multiprocessing
def symbol_to_csv(symbol,window=get_driver()):
    """ Takes in the symbol and returns floorsheet dataframe """
    # Add driver to path  
    scraper = Tungdung_serial(symbol,window)
    # Get the URL Pate
    os.environ["PATH"] += os.pathsep + r"driver"
    search_url: str = f"{BASE_URL}{symbol}"
    scraper.window.get(search_url)
    scraper.wait(3)
    nav_btn = scraper.better_id_selector("navFloorSheet")
    # Click on the nav button
    print(nav_btn)
    nav_btn.click()

    # Gets the frame and writes to csv File
    scraper.frames_to_csv(symbol)

    # dynamically Gets the Last Page Index
    last_page_no = scraper.get_pages()["last_page"]

    while scraper.current_index < last_page_no:
        # Increment the counter
        scraper.current_index = scraper.current_index + 1
        scraper.table_top_data = scraper.table_top_data + 100
        # Click the button
        index_link = f"changePageIndex('{scraper.current_index}','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_hdnCurrentPage','ctl00_ContentPlaceHolder1_CompanyDetail1_PagerControlFloorsheet1_btnPaging')"
        scraper.window.execute_script(index_link)
        # Add to CSV
        print(f"For the symbol {symbol} and page: {scraper.current_index}")
        scraper.frames_to_csv(symbol,no_index=True)
    
    # scraper.drop_and_continue()
    scraper.logger_failure('Done')
    scraper.window.quit()




if __name__ == "__main__":
    total_symbols = len(symbols)
    
    for sym in symbols:
        symbol_to_csv(sym)
