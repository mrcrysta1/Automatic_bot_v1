# ////////////////////Data Scrap from Pulse Website using from inputs ////////////
import csv
import logging
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WebScraper:
    def __init__(self, browser='chrome', csv_file='scraped_data.csv'):
        self.browser = browser
        self.csv_file = csv_file
        self.driver = self.setup_driver()

    def setup_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Run in headless mode
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def open_page(self, url):
        self.driver.get(url)
        time.sleep(15)  # Adjust wait time if necessary

    def select_dropdown(self, select_id, text=None, value=None):
        try:
            select_element = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, select_id))
            )
            WebDriverWait(self.driver, 20).until(
                EC.visibility_of(select_element)
            )
            select_object = Select(select_element)
            WebDriverWait(self.driver, 20).until(
                lambda driver: len(select_object.options) > 1  # Ensure options are populated
            )
            if text:
                select_object.select_by_visible_text(text)
            elif value:
                select_object.select_by_value(value)
            logging.info(f"Dropdown '{select_id}' selected successfully with value '{text or value}'.")
        except Exception as e:
            logging.error(f"Dropdown '{select_id}' not found or interaction failed: {e}")
            self.driver.save_screenshot('error_screenshot.png')

    def input_text(self, input_id, text):
        input_element = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.ID, input_id))
        )
        input_element.send_keys(text)
        logging.info(f"Text '{text}' input successfully into element '{input_id}'.")

    def click_button(self, button_id):
        button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.ID, button_id))
        )
        button.click()
        logging.info(f"Button '{button_id}' clicked successfully.")

    def scrape_current_page(self, writer):
        # Parse the page source with BeautifulSoup
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Extract table rows
        table = soup.find('table')  # Adjust selector as necessary
        if table:
            rows = table.find_all('tr')
            if rows:
                # Loop through rows and extract data
                for row in rows:
                    cols = row.find_all('td')
                    cols_data = [col.text.strip() for col in cols]

                    # Find button and extract its ID
                    button = row.find('button')
                    button_id = button.get('id') if button else ''
                    cols_data.append(button_id)

                    # Write the row data to the CSV file
                    writer.writerow(cols_data)
            else:
                logging.info("No rows found in the table.")
        else:
            logging.info("No table found on the page.")

    def is_next_button_enabled(self):
        try:
            next_button = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a.page-link i.fa.fa-angle-left'))
            )
            return 'disabled' not in next_button.get_attribute('class')
        except Exception as e:
            logging.error(f"Error checking next button status: {e}")
            return False

    def navigate_to_next_page(self):
        try:
            next_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.page-link i.fa.fa-angle-left'))
            )
            if self.is_next_button_enabled():
                next_button.click()
                time.sleep(15)  # Adjust wait time after clicking next button
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Error navigating to next page: {e}")
            return False

    def run(self, url, district, tehsil, editor_name):
        try:
            self.open_page(url)
            self.select_dropdown('districtSelect', text=district)
            self.select_dropdown('tehsilSelect', value=tehsil)
            self.input_text('editor', editor_name)
            self.click_button('searchButton')
            self.select_dropdown('ipp', text='100')
            
            # Open the CSV file for writing
            with open(self.csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # Write the header row
                writer.writerow(['Column1', 'Column2', 'Column3', 'free', 'Button ID'])
                
                # Initialize page counter
                page_number = 1
                
                # Scrape data from the first page
                self.scrape_current_page(writer)
                
                # Continue navigating and scraping until no more pages are available
                while self.navigate_to_next_page():
                    page_number += 1
                    logging.info(f"Scraping page {page_number}...")
                    self.scrape_current_page(writer)
                
                logging.info("All pages have been scraped.")
        
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        
        finally:
            self.driver.quit()
            logging.info("WebDriver closed.")

# Example usage
if __name__ == "__main__":
    scraper = WebScraper(browser='chrome', csv_file='scraped_data.csv')
    scraper.run(
        url="https://rod.pulse.gop.pk/index.html",
        district='ملتان',
        tehsil='86',
        editor_name='عبدالرشید'
    )
