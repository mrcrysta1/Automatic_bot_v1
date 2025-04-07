import sys
import io
import requests
import base64
import json
import csv
from collections import defaultdict

# Set the default encoding to UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Elasticsearch URL
url = "https://rodb.pulse.gop.pk/registry_index_3/_search"

# Authentication (Base64 encoded)
username = "secure.txt"
password = "read"
credentials = f"{username}:{password}"
auth_header = base64.b64encode(credentials.encode()).decode()

# Headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Basic {auth_header}'
}

# Default CNIC
cnic = "3630225149201"

def fetch_data_by_cnic(cnic, batch_size=10000, scroll_time="1m"):
    # Generate list of Tehsil IDs from 0 to 150 as strings
    tehsil_ids = [str(i) for i in range(151)]
    
    searchQuery = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"TehsilId": tehsil_ids}},
                    {
                        "nested": {
                            "path": "RegistryParties",
                            "query": {
                                "term": {"RegistryParties.CNIC": {"value": cnic}}
                            }
                        }
                    }
                ]
            }
        },
        "_source": [
            "Id", "UserId", "VendorId", "UserWorkQueMasterId", "RegisteredNumber", 
            "JildNumber", "PropertyNumber", "IsApproved", "IsJildCompleted", 
            "BahiNumber", "RegistryDate", "IsActive", "CreatedDate", 
            "ModifiedDate", "CreatedBy", "ModifiedBy", "TehsilId", "MauzaId", 
            "Address", "IsExported", "Area", "RegistryValue", 
            "RegistryExportImg", "RegistryType", "MauzaName", 
            "RegistryParties"
        ],
        "size": batch_size
    }

    # Initial scroll request
    scroll_url = url + "?scroll=" + scroll_time
    response = requests.post(scroll_url, headers=headers, data=json.dumps(searchQuery))
    
    if response.status_code == 200:
        data = response.json()
        scroll_id = data['_scroll_id']
        all_data = data['hits']['hits']
        grouped_data = defaultdict(list)

        # Continue scrolling until all data is retrieved
        while True:
            scroll_query = {
                "scroll": scroll_time,
                "scroll_id": scroll_id
            }

            scroll_response = requests.post(
                "https://rodb.pulse.gop.pk/_search/scroll",
                headers=headers,
                data=json.dumps(scroll_query)
            )

            if scroll_response.status_code == 200:
                scroll_data = scroll_response.json()
                hits = scroll_data['hits']['hits']
                
                if not hits:  # Stop if no more data
                    break

                all_data.extend(hits)  # Append the current batch to the overall data
                scroll_id = scroll_data['_scroll_id']
            else:
                print(f"Scroll request failed: {scroll_response.status_code}")
                break

        # Process data for grouped output
        for hit in all_data:
            source = hit['_source']
            registry_id = source.get("Id")
            
            # Process each party in RegistryParties
            for i, party in enumerate(source.get("RegistryParties", []), start=1):
                grouped_data[registry_id].append({
                    "Registry ID": registry_id,
                    "Registered Number": source.get("RegisteredNumber"),
                    "Mauza Name": source.get("MauzaName"),
                    "Registry Date": source.get("RegistryDate"),
                    "Property Number": source.get("PropertyNumber"),
                    f"Party {i} Name": party.get("Name"),
                    f"Party {i} Spouse Name": party.get("SpouseName"),
                    f"Party {i} CNIC": party.get("CNIC"),
                    f"Party {i} Type ID": party.get("RegistryPartiesTypeId")
                })

        return all_data, grouped_data
    else:
        print(f"Initial request failed: {response.status_code}")
        print(response.text)
        return [], {}

# Function to save raw data to a CSV file
def save_raw_data(filename, raw_data):
    if not raw_data:
        print("No raw data to save.")
        return

    fieldnames = ["Id", "UserId", "VendorId", "UserWorkQueMasterId", 
                  "RegisteredNumber", "JildNumber", "PropertyNumber", 
                  "IsApproved", "IsJildCompleted", "BahiNumber", 
                  "RegistryDate", "IsActive", "CreatedDate", 
                  "ModifiedDate", "CreatedBy", "ModifiedBy", 
                  "TehsilId", "MauzaId", "Address", 
                  "IsExported", "Area", "RegistryValue", 
                  "RegistryExportImg", "RegistryType", "MauzaName", 
                  "RegistryParties"]

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        
        for entry in raw_data:
            source = entry['_source']
            row = {field: source.get(field) for field in fieldnames}
            writer.writerow(row)

# Function to save grouped data to a CSV file
def save_grouped_data(filename, grouped_data):
    if not grouped_data:
        print("No grouped data to save.")
        return

    max_parties = max(len(party_list) for party_list in grouped_data.values())
    fieldnames = ["Registry ID", "Registered Number", "Mauza Name", "Registry Date", "Property Number"]
    for i in range(1, max_parties + 1):
        fieldnames.extend([f"Party {i} Name", f"Party {i} Spouse Name", f"Party {i} CNIC", f"Party {i} Type ID"])
    
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        
        for registry_id, party_list in grouped_data.items():
            row = {key: None for key in fieldnames}  # Initialize row with None values
            row.update({
                "Registry ID": registry_id,
                "Registered Number": party_list[0].get("Registered Number"),
                "Mauza Name": party_list[0].get("Mauza Name"),
                "Registry Date": party_list[0].get("Registry Date"),
                "Property Number": party_list[0].get("Property Number"),
            })
            
            # Populate each party's data in the row
            for i, party_data in enumerate(party_list, start=1):
                row[f"Party {i} Name"] = party_data.get(f"Party {i} Name")
                row[f"Party {i} Spouse Name"] = party_data.get(f"Party {i} Spouse Name")
                row[f"Party {i} CNIC"] = party_data.get(f"Party {i} CNIC")
                row[f"Party {i} Type ID"] = party_data.get(f"Party {i} Type ID")
            
            writer.writerow(row)

# Fetch all data with Scroll API
raw_data, grouped_data = fetch_data_by_cnic(cnic)

# Save raw data to CSV
raw_csv_filename = f"raw_data_cnic_{cnic}.csv"
if raw_data:
    save_raw_data(raw_csv_filename, raw_data)
    print(f"Raw data saved to {raw_csv_filename}")
else:
    print("No raw data was retrieved.")

# Save grouped data to CSV
clean_csv_filename = f"clean_result_cnic_{cnic}.csv"
if grouped_data:
    save_grouped_data(clean_csv_filename, grouped_data)
    print(f"Grouped data saved to {clean_csv_filename}")
else:
    print("No grouped data was retrieved.")