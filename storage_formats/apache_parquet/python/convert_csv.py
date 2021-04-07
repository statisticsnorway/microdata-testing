import csv

source_file = 'accumulated_data_300_million_rows.csv'
target_file = 'accumulated_data_300_million_rows_converted.csv'

with open(source_file, newline='') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=';')
    with open(target_file, 'w', newline='') as target_file:
        csv_writer = csv.writer(target_file, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        for row in csv_reader:
            row[2] = row[2].replace('-', '')
            row[3] = row[3].replace('-', '')
            start_year = row[3][:4]
            row.append(start_year)  # add start_year column to partition on it
            csv_writer.writerow(row)
