import csv
from datetime import datetime

# Lager en fil med id'er som kan brukes til id filtrering

filter_size = 1000000
source_file = 'accumulated_data_300_million_rows.csv'
target_file = 'accumulated_data_300_million_rows_id_filter.csv'

print("Start ", datetime.now())

my_set = set()
with open(source_file, newline='') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=';')
    for row in csv_reader:
        my_set.update([row[0]])
        if len(my_set) == filter_size:
            break

with open(target_file, 'w') as target_file:
    for id in my_set:
        target_file.write('%s\n' % id)

print("End ", datetime.now())