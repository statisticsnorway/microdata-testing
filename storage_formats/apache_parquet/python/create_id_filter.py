import csv

# Lager en fil med id'er som kan brukes til id filtrering

filter_size = 50
source_file = 'accumulated_data_100_rader.txt'
target_file = 'accumulated_data_100_rader_id_filter.txt'

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

print("Ferdig!")