import sys
import io
import requests
import base64
import json
import csv

# Set the default encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Elasticsearch URL
url = "https://rodb.pulse.gop.pk/registry_index_3/_msearch"

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

# Function to fetch data with pagination
def fetch_data(tehsil_id, start_date, end_date, name, size=100):  # Set size to 100
    all_results = []
    from_ = 0

    # First request to get the count of distinct IDs
    countQuery = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"TehsilId": {"value": tehsil_id}}},
                    {"nested": {
                        "path": "RegistryParties",
                        "query": {"bool": {"must": [
                            {"match_phrase": {"RegistryParties.Name": name}},
                            {"match_all": {}}
                        ]}}
                    }},
                    {"range": {"RegistryDate": {"gte": start_date, "lte": end_date}}}
                ]
            }
        },
        "aggs": {
            "distinct_count": {"cardinality": {"field": "Id"}}
        },
        "size": 0
    }

    countQuery_payload = f'{{"index":"registry_index_3"}}\n{json.dumps(countQuery)}\n'
    count_response = requests.post(url, headers=headers, data=countQuery_payload)
    
    if count_response.status_code == 200:
        count_data = count_response.json()
        distinct_count = count_data.get('responses', [{}])[0].get('aggregations', {}).get('distinct_count', {}).get('value', 0)
        print(f"Distinct count of 'Id': {distinct_count}")
    else:
        print("Failed to retrieve distinct count")
        print(count_response.text)
        return all_results

    # Fetch paginated results
    while True:
        searchQuery = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"TehsilId": {"value": tehsil_id}}},
                        {"nested": {
                            "path": "RegistryParties",
                            "query": {"bool": {"must": [
                                {"match_phrase": {"RegistryParties.Name": name}},
                                {"match_all": {}}
                            ]}}
                        }},
                        {"range": {"RegistryDate": {"gte": start_date, "lte": end_date}}}
                    ]
                }
            },
            "sort": [{"Id": {"order": "desc"}}],
            "_source": ["Id", "RegisteredNumber", "MauzaName", "RegistryDate"],
            "from": from_,
            "size": size  # Fetch 100 records at a time
        }
        
        # Combine the queries into a single payload
        combinedQuery = f'{{"index":"registry_index_3"}}\n{json.dumps(searchQuery)}\n'
        
        # Send the POST request
        response = requests.post(url, headers=headers, data=combinedQuery)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'responses' in data and 'hits' in data['responses'][0]:
                results = [hit['_source'] for hit in data['responses'][0]['hits']['hits']]
                
                if not results:  # No more results, break the loop
                    break
                
                all_results.extend(results)
                from_ += size
            else:
                print("Unexpected response format, 'hits' key missing.")
                break
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            print(response.text)
            break
    
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
start_date = "1947-08-14"
end_date = "2024-11-01"
party_name = "عبدالغفور"  # Name to match in RegistryParties
all_data = fetch_data(tehsil_id, start_date, end_date, party_name)

# Save to CSV
csv_filename = "registry_data.csv"
if all_data:
    save_to_csv(csv_filename, all_data)
    print(f"Data saved to {csv_filename}")
else:
    print("No data was retrieved, so no CSV file was created.")
