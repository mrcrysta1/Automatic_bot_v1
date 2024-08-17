import csv

# Input and output file paths
input_file = 'ids.csv'
output_file = 'urls.csv'

# Base URL
base_url = 'https://rod.pulse.gop.pk/details_page.html?I='

# Read IDs from the input file and construct URLs
with open(input_file, mode='r') as infile, open(output_file, mode='w', newline='') as outfile:
    csv_reader = csv.reader(infile)
    csv_writer = csv.writer(outfile)
    
    # Write header for output file
    csv_writer.writerow(['URL'])
    
    # Skip header row in input file
    next(csv_reader)
    
    # Process each row in the input file
    for row in csv_reader:
        id_value = row[0]
        url = f'{base_url}{id_value}'
        csv_writer.writerow([url])

print(f'URLs have been written to {output_file}')
