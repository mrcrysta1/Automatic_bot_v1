import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

# Set up the Selenium WebDriver
driver = webdriver.Chrome()

# Define the CSV file name
csv_file = 'scraped_data.csv'

try:
    # Open the webpage
    driver.get("https://rod.pulse.gop.pk/index.html")

    # Allow the page to load
    time.sleep(15)  # Increased wait time for page load

    # Fill in the form fields
    # Select district
    district_select = Select(WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, 'districtSelect'))))
    district_select.select_by_visible_text('ملتان')  # Selecting the district "ملتان"

    # Wait for the tehsil dropdown to populate
    time.sleep(5)  # Adjust the wait time as needed

    # Select tehsil by value (assuming value is known)
    tehsil_select = Select(WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, 'tehsilSelect'))))
    tehsil_select.select_by_value('87')  # Selecting the tehsil by its value, adjust as per the actual value

    # Enter editor name
    editor_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, 'editor')))
    editor_input.send_keys('اللہ داد')  # Entering the editor name "اللہ داد"

    # Submit the form by clicking the search button
    search_button = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, 'searchButton')))
    search_button.click()

    # Allow the results to load
    time.sleep(15)  # Increased wait time for results to load

    # Set the records per page to 100
    records_per_page_select = Select(WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, 'ipp'))))  # Using the correct selector ID
    records_per_page_select.select_by_visible_text('100')

    # Allow the page to reload with the new settings
    time.sleep(15)  # Increased wait time after setting records per page

    # Open the CSV file for writing
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write the header row if needed (adjust column names as necessary)
        writer.writerow(['Column1', 'Column2', 'Column3','free ', 'Button ID'])  # Adjust column names as needed

        # Function to scrape data from the current page
        def scrape_current_page():
            # Parse the page source with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
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
                    print("No rows found in the table.")
            else:
                print("No table found on the page.")
        
        # Function to check if the "Next" button is enabled
        def is_next_button_enabled():
            try:
                next_button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.page-link i.fa.fa-angle-left')))
                return 'disabled' not in next_button.get_attribute('class')
            except Exception as e:
                print(f"Error checking next button status: {e}")
                return False
        
        # Function to navigate to the next page
        def navigate_to_next_page():
            try:
                next_button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.page-link i.fa.fa-angle-left')))
                if is_next_button_enabled():
                    next_button.click()
                    time.sleep(15)  # Adjust wait time after clicking next button
                    return True
                else:
                    return False
            except Exception as e:
                print(f"Error navigating to next page: {e}")
                return False
        
        # Scrape data from the first page
        scrape_current_page()

        # Continue navigating and scraping until no more pages are available
        while navigate_to_next_page():
            scrape_current_page()

except Exception as e:
    print("Error occurred:", e)

finally:
    # Close the WebDriver
    driver.quit()