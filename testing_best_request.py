import requests
import json
import base64
import csv

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

# Inputs for First Party and Second Party Names & Spouse Names
tehsil_id = "86"  # Example Tehsil ID
first_party_name = "ایوب"  # Example first party name
first_spouse_name = "عبدالطیف"  # Example first spouse name
second_party_name = ""  # Example second party name
second_spouse_name = ""  # Example second spouse name
address = ""  # Example address (optional)

# Function to fetch data with pagination
def fetch_data_with_scroll(tehsil_id, first_party_name, first_spouse_name, second_party_name, second_spouse_name, address, batch_size=10000, scroll_time="1m"):
    must_conditions = [
        {"term": {"TehsilId": {"value": tehsil_id}}},
        {
            "nested": {
                "path": "RegistryParties",
                "query": {
                    "bool": {
                        "must": [
                            {"match_phrase": {"RegistryParties.Name": first_party_name}},
                            {"match_phrase": {"RegistryParties.SpouseName": first_spouse_name}}
                        ]
                    }
                }
            }
        }
    ]

    # Add second party details if available
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

    # Only add address to the query if it is not empty
    if address:
        must_conditions.append({"match": {"Address": address}})

    searchQuery = {
        "query": {
            "bool": {
                "must": must_conditions
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
        "size": batch_size  # Set batch size to 10,000 or less
    }

    # Initial scroll request
    scroll_url = "https://rodb.pulse.gop.pk/registry_index_3/_search?scroll=" + scroll_time
    response = requests.post(scroll_url, headers=headers, data=json.dumps(searchQuery))

    if response.status_code == 200:
        data = response.json()
        scroll_id = data['_scroll_id']
        all_data = data['hits']['hits']

        # Continue scrolling and fetching data until all is retrieved
        while True:
            scroll_query = {
                "scroll": scroll_time,
                "scroll_id": scroll_id
            }

            scroll_response = requests.post("https://rodb.pulse.gop.pk/_search/scroll", headers=headers, data=json.dumps(scroll_query))

            if scroll_response.status_code == 200:
                scroll_data = scroll_response.json()
                hits = scroll_data['hits']['hits']
                if not hits:  # If no more results are found, stop
                    break

                all_data.extend(hits)
                scroll_id = scroll_data['_scroll_id']
            else:
                print(f"Failed to retrieve data using scroll: {scroll_response.status_code}")
                break

        return all_data
    else:
        print(f"Initial request failed: {response.status_code}")
        print(response.text)
        return []

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

# Fetch all data using Scroll API
all_raw_data = fetch_data_with_scroll(tehsil_id, first_party_name, first_spouse_name, second_party_name, second_spouse_name, address)

# Save raw data to CSV
raw_csv_filename = "all_raw_data.csv"
if all_raw_data:
    save_raw_data(raw_csv_filename, all_raw_data)
    print(f"All data saved to {raw_csv_filename}")
else:
    print("No data retrieved.")
