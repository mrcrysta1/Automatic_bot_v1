import csv

# Input and output file paths
input_file = 'registry_data.csv'
# input_file = 'scraped_data.csv'
output_file = 'ids.csv'

# Initialize an empty list to store Button IDs
button_ids = []

# Read the input CSV file and extract Button IDs
with open(input_file, mode='r', encoding='utf-8-sig') as file:
    reader = csv.DictReader(file)
    for row in reader:
        button_ids.append([row['Id']])

# Write Button IDs to a new CSV file
with open(output_file, mode='w', encoding='utf-8-sig', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(button_ids)

print(f"New CSV file '{output_file}' has been created with Button IDs.")
