import requests
from bs4 import BeautifulSoup
import urllib.parse
import xml.etree.ElementTree as ET

# Set the base URL of your website
base_url = 'https://rodb.pulse.gop.pk/'

# Function to crawl and find all links
def get_all_links(url):
    try:
        # Send GET request to the page
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        
        # Parse the page content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all anchor tags with href attributes
        links = soup.find_all('a', href=True)
        
        # Extract the href links
        href_links = [link['href'] for link in links]
        
        # Normalize and get absolute URLs
        absolute_links = [urllib.parse.urljoin(base_url, href) for href in href_links]
        
        return absolute_links
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page: {e}")
        return []

# Fetch all links from the website
all_links = get_all_links(base_url)

# Display the links (routes)
print("Links from crawling the website:")
for link in all_links:
    print(link)

#--------------------------------------------------------------------------------------
# Sitemap URL (replace with your website's sitemap URL)
sitemap_url = f'{base_url}/sitemap.xml'  # Assuming the sitemap is located at /sitemap.xml

# Function to fetch and parse the sitemap
def get_sitemap_urls(sitemap_url):
    try:
        response = requests.get(sitemap_url)
        response.raise_for_status()  # Check if the request was successful
        
        # Parse the XML content of the sitemap
        root = ET.fromstring(response.text)
        
        # Extract all <loc> elements (which contain the URLs)
        urls = [url.text for url in root.findall(".//url/loc")]
        
        return urls
    except requests.exceptions.RequestException as e:
        print(f"Error fetching sitemap: {e}")
        return []

# Fetch URLs from the sitemap
sitemap_urls = get_sitemap_urls(sitemap_url)

# Display the URLs from sitemap
print("\nLinks from sitemap:")
for url in sitemap_urls:
    print(url)
