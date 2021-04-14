import pandas
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

csv_filename = "accumulated_data_300_million_rows_converted.csv"
parquet_filename = '../data/' + csv_filename.replace('csv', 'parquet')
parquet_partition_name = '../data/' + csv_filename.replace('.csv', '')

print("Start ", datetime.now())

# ReadOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ReadOptions.html#pyarrow.csv.ReadOptions
csv_read_options = pv.ReadOptions(
    skip_rows=0,
    encoding="utf8",
    column_names=["unit_id", "value", "start", "stop", "start_year", "start_unix_days", "stop_unix_days"])

# ParseOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html#pyarrow.csv.ParseOptions
csv_parse_options = pv.ParseOptions(delimiter=';')

# Types: https://arrow.apache.org/docs/python/api/datatypes.html
# TODO nullable parameter does not work as expected!
data_schema = pa.schema([
    pa.field(name='start_year', type=pa.string(), nullable=True),
    pa.field(name='unit_id', type=pa.uint64(), nullable=False),
    pa.field(name='value', type=pa.string(), nullable=False),
    pa.field(name='start_epoch_days', type=pa.int16(), nullable=True),
    pa.field(name='stop_epoch_days', type=pa.int16(), nullable=True),
])

# ConvertOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html#pyarrow.csv.ConvertOptions
csv_convert_options = pv.ConvertOptions(column_types=data_schema,
                                        include_columns=["unit_id", "value", "start_year", "start_unix_days", "stop_unix_days"])

# read_csv: https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html#pyarrow.csv.read_csv
table = pv.read_csv(input_file=csv_filename, read_options=csv_read_options, parse_options=csv_parse_options,
                    convert_options=csv_convert_options)

print(table.nbytes)
print(table.num_rows)
print(table.schema)
print(table.column_names)
pandas.set_option('max_columns', None) # print all columns
print(table.to_pandas().head(10))

# write_table: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
# pq.write_table(table, parquet_filename)

# Write a dataset and collect metadata information of all written files
metadata_collector = []

# write with partitions
pq.write_to_dataset(table,
                    root_path=parquet_partition_name,
                    partition_cols=['start_year'],
                    metadata_collector=metadata_collector)

# Write the ``_common_metadata`` parquet file without row groups statistics
# pq.write_metadata(table.schema, parquet_partition_name + '/_common_metadata')

print("End ", datetime.now())