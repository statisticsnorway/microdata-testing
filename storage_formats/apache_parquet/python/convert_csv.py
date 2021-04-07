import csv
from datetime import datetime

source_file = 'accumulated_data_300_million_rows.csv'
target_file = 'accumulated_data_300_million_rows_converted.csv'


def days_since_epoch(date_string: str) -> int:
    epoch = datetime.utcfromtimestamp(0)
    date_obj = datetime.strptime(date_string, '%Y%m%d')
    return (date_obj - epoch).days

print("Start ", datetime.now())

with open(source_file, newline='') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=';')
    with open(target_file, 'w', newline='') as target_file:
        csv_writer = csv.writer(target_file, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        for row in csv_reader:
            start_date: str = row[2]
            stop_date: str = row[3]

            start_date = start_date.replace('-', '')
            stop_date = stop_date.replace('-', '')
            row[2] = start_date
            row[3] = stop_date

            start_year = start_date[:4]
            row.append(start_year)  # add start_year column to partition on it

            row.append(days_since_epoch(start_date))
            row.append(days_since_epoch(stop_date))

            csv_writer.writerow(row)

print("End ", datetime.now())
