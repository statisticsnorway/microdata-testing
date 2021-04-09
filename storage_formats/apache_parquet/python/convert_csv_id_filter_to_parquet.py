import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyarrow.lib import Table

csv = 'accumulated_data_300_million_rows_id_filter.csv'
target_file = '../data/accumulated_data_300_million_rows_id_filter_1mill.parquet'

csv_read_options = pv.ReadOptions(
    skip_rows=0,
    encoding="utf8",
    column_names=["unit_id"])

# Types: https://arrow.apache.org/docs/python/api/datatypes.html
data_schema = pa.schema([
    ('unit_id', pa.uint64())
])

# ConvertOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html#pyarrow.csv.ConvertOptions
csv_convert_options = pv.ConvertOptions(column_types=data_schema)

table: Table = pv.read_csv(input_file=csv, read_options=csv_read_options, convert_options=csv_convert_options)
pq.write_table(table, target_file)

print('Generated file with the following:')
print('Parquet metadata: ' + str(pq.read_metadata(target_file)))
print('Parquet schema: ' + pq.read_schema(target_file).to_string())

