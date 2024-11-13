def test_fetch_data():
    # Sample input values
    tehsil_id = "87"  # Test Tehsil ID
    party_name = "عبد الحمید"  # Test party name
    spouse_name = "گورنر پاکستان"  # Test spouse name 
    address = ""  # Test address

    # Call the function to fetch data
    raw_data, grouped_data = fetch_data(tehsil_id, party_name, spouse_name, address)
    
    # Print raw data
    print("Raw Data:")
    for entry in raw_data:
        print(json.dumps(entry, ensure_ascii=False, indent=4))  # Pretty print with UTF-8 support

    # Print grouped data
    print("\nGrouped Data:")
    for registry_id, parties in grouped_data.items():
        print(f"Registry ID: {registry_id}")
        for party in parties:
            print(json.dumps(party, ensure_ascii=False, indent=4))  # Pretty print with UTF-8 support

# Run the test function
test_fetch_data()
