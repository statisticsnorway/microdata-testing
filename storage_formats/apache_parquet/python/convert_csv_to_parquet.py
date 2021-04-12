import pandas
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime
from timer import timeblock
# from google.cloud.bigquery.schema import SchemaField


root_dir = "/Users/vak/temp/"
file_name = "data_50_missing_converted.csv"

csv_filename = root_dir + file_name
parquet_filename = root_dir + 'data/' + file_name.replace('csv', 'parquet')
parquet_partition_name = root_dir + 'data/' + file_name.replace('.csv', '')

print('csv_filename : ' + csv_filename)
print('parquet_filename : ' + parquet_filename)
print('parquet_partition_name : ' + parquet_partition_name)

# exit(5)

print("Start ", datetime.now())

# ReadOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ReadOptions.html#pyarrow.csv.ReadOptions
csv_read_options = pv.ReadOptions(
    skip_rows=0,
    encoding="utf8",
    column_names=["unit_id", "value", "start", "stop", "start_year", "start_unix_days", "stop_unix_days"])

# ParseOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html#pyarrow.csv.ParseOptions
csv_parse_options = pv.ParseOptions(delimiter=';')

# Types: https://arrow.apache.org/docs/python/api/datatypes.html

# schema_by_schema_fields = [
#     SchemaField("unit_id", "INTEGER", mode="REQUIRED"),
#     SchemaField("value", "STRING", mode="REQUIRED"),
#     SchemaField("start_year", "INTEGER", mode="REQUIRED"),
#     SchemaField("start_unix_days", "INTEGER", mode="REQUIRED"),
#     SchemaField("stop_unix_days", "INTEGER", mode="NULLABLE"),
# ]
#
# data_schema = pa.schema(schema_by_schema_fields)

data_schema = pa.schema([
    ('unit_id', pa.uint64()),
    ('value', pa.string()),
    ('start_year', pa.uint16()),
    ('start_unix_days', pa.uint16()),  # more than AD 2100
    ('stop_unix_days', pa.uint16()),
])

# ConvertOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html#pyarrow.csv.ConvertOptions
csv_convert_options = pv.ConvertOptions(
                            column_types=data_schema,
                            include_columns=["unit_id", "value", "start_year", "start_unix_days", "stop_unix_days"],
                            strings_can_be_null=False)

# read_csv: https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html#pyarrow.csv.read_csv
table = pv.read_csv(input_file=csv_filename, read_options=csv_read_options, parse_options=csv_parse_options,
                    convert_options=csv_convert_options)

print("--------------------")
print(table.nbytes)
print(table.num_rows)
print(table.schema)
print(table.column_names)
pandas.set_option('max_columns', None) # print all columns
print(table.to_pandas().head(10))
print("--------------------")

# write_table: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
# pq.write_table(table, parquet_filename)

# Write a dataset and collect metadata information of all written files
metadata_collector = []

# write with partitions
with timeblock('Write table to partitioned parquet files'):
    pq.write_to_dataset(table, root_path=parquet_partition_name, partition_cols=['start_year'])

print("End ", datetime.now())