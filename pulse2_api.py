# unlimited data fecth using api with privat api key
import sys
import io
import requests
import base64
import json
import csv

# Set the default encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Elasticsearch URL
url = "https://rodb.pulse.gop.pk/registry_index_2/_search?scroll=1m"

# Authentication (Base64 encoded)
username = "elastic"
password = "qZM2ov-qIa=UXr6+Gx8b"
credentials = f"{username}:{password}"
auth_header = base64.b64encode(credentials.encode()).decode()

# Headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Basic {auth_header}'
}

# Function to initiate the scroll and fetch data
def fetch_data(tehsil_id, start_date, end_date, size=10000):  # Fetch 10,000 records at a time
    all_results = []
    total_retrieved = 0
    
    searchQuery = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"TehsilId": {"value": tehsil_id}}},
                    {"nested": {
                        "path": "RegistryParties",
                        "query": {"bool": {"must": [{"match_all": {}}, {"match_all": {}}]}}
                    }},
                    {"range": {"RegistryDate": {"gte": start_date, "lte": end_date}}}
                ]
            }
        },
        "sort": [{"Id": {"order": "desc"}}],
        "_source": ["Id", "RegisteredNumber", "MauzaName", "RegistryDate"],
        "size": size
    }
    
    # Send the initial POST request to start scrolling
    response = requests.post(url, headers=headers, data=json.dumps(searchQuery))
    
    if response.status_code == 200:
        data = response.json()
        scroll_id = data['_scroll_id']
        hits = data['hits']['hits']
        
        all_results.extend([hit['_source'] for hit in hits])
        total_retrieved += len(hits)
        
        # Continue fetching more data until we have 100,000 records or no more data
        while hits and total_retrieved < 100000:
            scroll_url = "https://rodb.pulse.gop.pk/_search/scroll"
            scroll_query = {
                "scroll": "1m",
                "scroll_id": scroll_id
            }
            response = requests.post(scroll_url, headers=headers, data=json.dumps(scroll_query))
            
            if response.status_code == 200:
                data = response.json()
                scroll_id = data['_scroll_id']
                hits = data['hits']['hits']
                
                if not hits:
                    break
                
                all_results.extend([hit['_source'] for hit in hits])
                total_retrieved += len(hits)
            else:
                print(f"Failed to retrieve data during scroll: {response.status_code}")
                print(response.text)
                break
    else:
        print(f"Failed to initiate scroll: {response.status_code}")
        print(response.text)
    
    return all_results

# Function to save data to a CSV file
def save_to_csv(filename, data):
    keys = data[0].keys() if data else ["Id", "RegisteredNumber", "MauzaName", "RegistryDate"]
    
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

# Fetch the data
tehsil_id = "86"
start_date = "1000-08-14"
end_date = "1970-08-12"
all_data = fetch_data(tehsil_id, start_date, end_date)

# Save to CSV
csv_filename = "registry_data.csv"
if all_data:
    save_to_csv(csv_filename, all_data)
    print(f"Data saved to {csv_filename}")
else:
    print("No data was retrieved, so no CSV file was created.")
