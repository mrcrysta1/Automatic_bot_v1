import requests
import json
import base64

# Elasticsearch URL and credentials
url = "https://rodb.pulse.gop.pk/registry_index_3/_search"
username = "elastic"
password = "qZM2ov-qIa=UXr6+Gx8b"
credentials = f"{username}:{password}"
auth_header = base64.b64encode(credentials.encode()).decode()

# Headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Basic {auth_header}'
}

# Define search parameters
tehsil_id = "86"  # Example Tehsil ID
party_name = "ایوب طاہر"  # Example party name to search
spouse_name = "عبدالطیف"  # Example spouse name to search

party_name2 = "محبوب"  # Optional second party name
spouse_name2 = "عبدالطیف"  # Optional second spouse name

# party_name2 = ""  # Optional second party name
# spouse_name2 = ""  # Optional second spouse name

# Construct Elasticsearch query
search_query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"TehsilId": {"value": tehsil_id}}},
                {
                    "nested": {
                        "path": "RegistryParties",
                        "query": {
                            "bool": {
                                "must": [
                                    {"match_phrase": {"RegistryParties.Name": party_name}},
                                    {"match_phrase": {"RegistryParties.SpouseName": spouse_name}}
                                ]
                            }
                        }
                    }
                }
            ]
        }
    },
    "_source": [
        "Id", "RegisteredNumber", "MauzaName", "RegistryDate", "PropertyNumber",
        "RegistryParties.Name", "RegistryParties.SpouseName", "RegistryParties.CNIC",
        "RegistryParties.RegistryPartiesTypeId"
    ],
    "size": 10
}

# Add optional second party criteria if specified
if party_name2 and spouse_name2:
    search_query["query"]["bool"]["must"].append({
        "nested": {
            "path": "RegistryParties",
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"RegistryParties.Name": party_name2}},
                        {"match_phrase": {"RegistryParties.SpouseName": spouse_name2}}
                    ]
                }
            }
        }
    })

# Send the POST request
response = requests.post(url, headers=headers, data=json.dumps(search_query))

# Check response
if response.status_code == 200:
    data = response.json()
    for hit in data["hits"]["hits"]:
        print(hit["_source"])  # Print each matching document's source data
else:
    print("Failed to retrieve data:", response.status_code)
    print(response.text)
