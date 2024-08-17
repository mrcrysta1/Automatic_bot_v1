import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import time

# Configure logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.DEBUG,  # Set to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize the Selenium WebDriver with options
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run in headless mode for better performance
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)

# Step 1: Read URLs from urls.csv
url_file = 'urls.csv'
urls = pd.read_csv(url_file)

# Prepare a list to hold the data
data_list = []

# Initialize a counter for successfully processed URLs
success_count = 0

# Step 2: Scrape data from each URL
for url in urls['url']:
    try:
        logging.info(f"Fetching URL: {url}")
        driver.get(url)
        
        # Wait for the card element to be present
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'card'))
        )
        
        # Get the page source and parse it with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Locate the div by class
        div = soup.find('div', class_='card card-statistics urdu text-center d-md-inline')
        
        if div:
            logging.info("Div found with class 'card card-statistics urdu text-center d-md-inline'")
            # Extract relevant spans
            span_registry_id = div.find('span', id='registryIdPlaceholder')
            registry_id = span_registry_id.get_text(strip=True) if span_registry_id else 'No registry ID'
            logging.info(f"Registry ID: {registry_id}")
        else:
            logging.warning("Div not found with class 'card card-statistics urdu text-center d-md-inline'")
            registry_id = 'No registry ID'
        
        # Locate the table by ID
        table = soup.find('table', id='urduTable')
        
        if table:
            logging.info("Table found with ID 'urduTable'")
            # Extract table headers
            headers = [header.get_text(strip=True) for header in table.find_all('th')]
            
            # Extract table rows
            rows = []
            for row in table.find_all('tr')[1:]:  # Skip the header row
                cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
                if cells:  # Only add rows with data
                    rows.append(cells)
            
            # Append the data to the list
            for row in rows:
                data = dict(zip(headers, row))
                data['url'] = url  # Add the URL to track the source
                data['registry_id'] = registry_id
                data_list.append(data)
        else:
            logging.warning("Table not found with ID 'urduTable'")
            # Append the data with no table found
            data_list.append({
                'url': url,
                'تفصیلات برائے رجسٹری نمبر': registry_id,
                'پراپرٹی ایڈریس': 'No data',
                'تاریخ رجسٹری': 'No data',
                'رجسٹری کی قسم': 'No data',
                'جلد نمبر': 'No data'
            })
        
        # Locate the cards with party information
        cards = soup.find_all('div', class_='card col-md-3 m-2 rtl')
        
        for card in cards:
            card_body = card.find('div', class_='card-body')
            
            if card_body:
                party_name = card_body.find('h5', class_='card-title urdu2').get_text(strip=True)
                
                # Extracting party information
                id_number = 'No ID'
                party_type = 'No type'
                father_name = 'No name'
                
                for p in card_body.find_all('p'):
                    span_text = p.find('span').get_text(strip=True)
                    text_content = p.get_text(strip=True).replace(span_text, '').strip()
                    if 'شناختی کارڈ نمبر:' in span_text:
                        id_number = text_content
                    elif 'پارٹی کی قسم:' in span_text:
                        party_type = text_content
                    elif 'شوہر/والد کا نام:' in span_text:
                        father_name = text_content
                
                data_list.append({
                    'url': url,
                    'registry_id': registry_id,
                    'party_name': party_name,
                    'id_number': id_number,
                    'party_type': party_type,
                    'father_name': father_name
                })
            else:
                logging.warning("Card body not found")
        
        # Increment the success counter
        success_count += 1
        logging.info(f"Successfully processed URL: {url}")
        
        # Introduce a delay to avoid hitting the server too fast
        time.sleep(2)  # Delay for 2 seconds
    
    except TimeoutException:
        logging.error(f"Timeout while processing {url}")
    except NoSuchElementException:
        logging.error(f"Element not found while processing {url}")
    except Exception as e:
        logging.error(f"Error processing {url}: {e}")

# Step 3: Store the data in database.csv
try:
    database_file = 'database.csv'
    df = pd.DataFrame(data_list)
    df.to_csv(database_file, index=False, encoding='utf-8-sig')
    logging.info(f"Data scraped and saved to {database_file}")
except Exception as e:
    logging.error(f"Failed to save data to {database_file}: {e}")

logging.info(f"Total successfully processed URLs: {success_count}")

# Quit the Selenium WebDriver
driver.quit()
