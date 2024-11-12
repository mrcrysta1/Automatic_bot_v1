# this script find data with input address and city 
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
username = "elastic"
password = "qZM2ov-qIa=UXr6+Gx8b"
credentials = f"{username}:{password}"
auth_header = base64.b64encode(credentials.encode()).decode()

# Headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Basic {auth_header}'
}

# Inputs
tehsil_id = "86"  # Example Tehsil ID
property_number = "طرف راوی"  # Example Property Number

# Function to fetch data without limit
def fetch_data(tehsil_id, property_number):
    searchQuery = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"TehsilId": {"value": tehsil_id}}},
                    {"match_phrase": {"PropertyNumber": property_number}}  # Search by Property Number
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
        "size": 10000  # Large fetch size; Elasticsearch may limit based on server settings
    }
    
    # Send the POST request
    response = requests.post(url, headers=headers, data=json.dumps(searchQuery))
    
    if response.status_code == 200:
        data = response.json()
        raw_data = []
        grouped_data = defaultdict(list)
        
        if 'hits' in data and 'hits' in data['hits']:
            for hit in data['hits']['hits']:
                source = hit['_source']
                registry_id = source.get("Id")
                
                # Add to raw data list
                raw_data.append(source)
                
                # Process for grouped data
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
                    
        return raw_data, grouped_data
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        print(response.text)
        return [], {}

# Function to save raw data to a CSV file
def save_raw_data(filename, raw_data):
    if not raw_data:
        print("No raw data to save.")
        return

    # Determine all columns in raw data
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
            row = {field: entry.get(field) for field in fieldnames}
            writer.writerow(row)

# Function to save grouped data to a CSV file
def save_grouped_data(filename, grouped_data):
    if not grouped_data:
        print("No grouped data to save.")
        return

    # Determine all column headers based on the maximum number of parties in any registry
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

# Fetch data based on PropertyNumber
raw_data, grouped_data = fetch_data(tehsil_id, property_number)

# Save raw data to raw_data.csv
raw_csv_filename = "raw_data.csv"
if raw_data:
    save_raw_data(raw_csv_filename, raw_data)
    print(f"Raw data saved to {raw_csv_filename}")
else:
    print("No raw data was retrieved.")

# Save grouped data to clean_result.csv
clean_csv_filename = "clean_result.csv"
if grouped_data:
    save_grouped_data(clean_csv_filename, grouped_data)
    print(f"Grouped data saved to {clean_csv_filename}")
else:
    print("No grouped data was retrieved.")
