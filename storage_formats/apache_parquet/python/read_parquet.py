import pandas
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime
from timer import timeblock
from memory_profiler import profile
# from google.cloud.bigquery.schema import SchemaField


@profile()
def read_file():
    #file = "/Users/vak/temp/data/data_50_missing_converted/start_year=1972/30e0862b47bb446086b0df653a6bebe9.parquet"
    #file = "/Users/vak/temp/data/resultsets/1618218150734run_partition_test2_result_set.parquet"
    file="/Users/vak/temp/data/accumulated_data_1_million_rader_converted_int16"

    print("Start ", datetime.now())

    data = pq.read_table(source=file)

    print("Metadata from result file")
    print(data.nbytes)
    print(data.num_columns)
    print(data.num_rows)
    print(data.schema)
    print(data.column_names)
    pandas.set_option('max_columns', None) # print all columns
    print(data.to_pandas().head(30))

    print("End ", datetime.now())

read_file()