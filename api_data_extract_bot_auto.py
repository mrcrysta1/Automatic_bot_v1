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
party_name = ""  # Example party name
spouse_name = ""  # Example spouse name 
address = ""  # Example address
second_party_name = ""  # Optional second party name
second_spouse_name = ""  # Optional second spouse name
RegistryDate = "2015-04-23"
Cnic = ""
# Function to fetch data without limit
def fetch_data(tehsil_id, party_name, spouse_name, address, second_party_name, second_spouse_name , RegistryDate, Cnic):
    must_conditions = [
        {"term": {"TehsilId": {"value": tehsil_id}}}
    ]
    
    if party_name:
        must_conditions.append({
            "nested": {
                "path": "RegistryParties",
                "query": {
                    "bool": {
                        "must": [{"match_phrase": {"RegistryParties.Name": party_name}}]
                    }
                }
            }
        })
    if Cnic:
        must_conditions.append({
            "nested": {
                "path": "RegistryParties",
                "query": {
                    "bool": {
                        "must": [{"match_phrase": {"RegistryParties.Cnic": Cnic}}]
                    }
                }
            }
        })

    # Only add second party conditions if the second party name and spouse name are provided
    if second_party_name and second_spouse_name:
        must_conditions.append({
            "nested": {
                "path": "RegistryParties",
                "query": {
                    "bool": {
                        "must": [
                            {"match_phrase": {"RegistryParties.Name": second_party_name}},
                            {"match_phrase": {"RegistryParties.SpouseName": second_spouse_name}}
                        ]
                    }
                }
            }
        })
    # Add spouse name condition if it is provided
    if spouse_name:
        must_conditions[1]["nested"]["query"]["bool"]["must"].append(
            {"match_phrase": {"RegistryParties.SpouseName": spouse_name}}
        )

    # Only add address to the query if it is not empty
    if address:
        must_conditions.append({"match": {"PropertyNumber": address}})
   # Only add Registry Date to the query if it is not empty
    if RegistryDate:
        must_conditions.append({"match": {"RegistryDate": RegistryDate}})

    searchQuery = {
        "query": {
            "bool": {
                "must": must_conditions
            }
        },
        "_source": [
            "Id", "UserId", "VendorId", "UserWorkQueMasterId", "RegisteredNumber", 
            "JildNumber", "PropertyNumber", "IsApproved", "IsJildCompleted", 
            "BahiNumber", "RegistryType" , "RegistryDate", "IsActive", "CreatedDate", 
            "ModifiedDate", "CreatedBy", "ModifiedBy", "TehsilId", "MauzaId", 
             "IsExported", "Area", "RegistryValue", 
            "RegistryExportImg", "RegistryType", "MauzaName", 
            "RegistryParties"
        ],
        "size": 10000
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
                        "Registry Type": source.get("RegistryType"),
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
                  "RegistryDate", "PartyType" , "IsActive", "CreatedDate", 
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
    fieldnames = ["Registry ID", "Registered Number", "Mauza Name", "Registry Date", "Property Number" , "Registry Type"]
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
                "Registry Type": party_list[0].get("Registry Type"),

            })
            
            # Populate each party's data in the row
            for i, party_data in enumerate(party_list, start=1):
                row[f"Party {i} Name"] = party_data.get(f"Party {i} Name")
                row[f"Party {i} Spouse Name"] = party_data.get(f"Party {i} Spouse Name")
                row[f"Party {i} CNIC"] = party_data.get(f"Party {i} CNIC")
                row[f"Party {i} Type ID"] = party_data.get(f"Party {i} Type ID")
                
            
            writer.writerow(row)

# Fetch data without limits
raw_data, grouped_data = fetch_data(tehsil_id, party_name, spouse_name, address, second_party_name, second_spouse_name, RegistryDate, Cnic)

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
