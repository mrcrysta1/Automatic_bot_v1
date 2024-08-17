import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

driver = webdriver.Chrome()
# Step 1: Read URLs from urls.csv
url_file = 'main_urls.csv'
urls = pd.read_csv(url_file)

# Prepare a list to hold the data
data_list = []

# Step 2: Scrape data from each URL
for url in urls['url']:
    try:
        print(f"Fetching URL: {url}")
        driver.get(url)
        time.sleep('1')
        # Wait for the page to fully load
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'card')))
        
        # Get the page source and parse it with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Locate the div by class
        div = soup.find('div', class_='card card-statistics urdu text-center d-md-inline')
        
        if div:
            print("Div found with class 'card card-statistics urdu text-center d-md-inline'")
            # Extract relevant spans
            span_registry_id = div.find('span', id='registryIdPlaceholder')
            registry_id = span_registry_id.get_text(strip=True) if span_registry_id else 'No registry ID'
            print(f"Registry ID: {registry_id}")
        else:
            print("Div not found with class 'card card-statistics urdu text-center d-md-inline'")
            registry_id = 'No registry ID'
        
        # Locate the table by ID
        table = soup.find('table', id='urduTable')
        
        if table:
            print("Table found with ID 'urduTable'")
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
            print("Table not found with ID 'urduTable'")
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
                party_info = {}
                for span in card_body.find_all('span'):
                    span_text = span.get_text(strip=True)
                    if 'شناختی کارڈ نمبر:' in span_text:
                        party_info['id_number'] = span.find_next_sibling(text=True).strip()
                    elif 'پارٹی کی قسم:' in span_text:
                        party_info['party_type'] = span.find_next_sibling(text=True).strip()
                    elif 'شوہر/والد کا نام:' in span_text:
                        party_info['father_name'] = span.find_next_sibling(text=True).strip()
                    else:
                        continue
                
                data_list.append({
                    'url': url,
                    'registry_id': registry_id,
                    'party_name': party_name,
                    'id_number': party_info.get('id_number', 'No ID'),
                    'party_type': party_info.get('party_type', 'No type'),
                    'father_name': party_info.get('father_name', 'No name')
                })
            else:
                print("Card body not found")
        print(f"Successfully processed URL: {url}")
    
    except Exception as e:
        print(f"Error processing {url}: {e}")

# Step 3: Store the data in database.csv
database_file = 'database.csv'
df = pd.DataFrame(data_list)
df.to_csv(database_file, index=False, encoding='utf-8-sig')

print(f"Data scraped and saved to {database_file}")

# Quit the Selenium WebDriver
driver.quit()
