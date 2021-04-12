import pandas
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime
from timer import timeblock
# from google.cloud.bigquery.schema import SchemaField


# file = "/Users/vak/temp/data/data_50_missing_converted/start_year=1972/b167f65b0fda4145b2651a61e3d66799.parquet"
file = "/Users/vak/temp/data/resultsets/1618213673441run_partition_test2_result_set.parquet"

print("Start ", datetime.now())

data = pq.read_table(source=file)

print("Metadata from result file")
print(data.nbytes)
print(data.num_rows)
print(data.schema)
print(data.column_names)
pandas.set_option('max_columns', None) # print all columns
print(data.to_pandas().head(30))


print("End ", datetime.now())