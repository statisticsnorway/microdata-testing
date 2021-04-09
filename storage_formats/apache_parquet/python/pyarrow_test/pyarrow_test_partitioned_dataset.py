from time import time
from typing import Optional
import pandas

import pyarrow.parquet as pq
from memory_profiler import profile
from timer import timeblock


@profile()
def run_partition_test2(input_file_root_path: str, output_dir: str, filters: Optional[list] = None):

    with timeblock('Read and filter'):
        table = pq.read_table(source=input_file_root_path, filters=filters, use_threads=False)

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + 'run_partition_test2_result_set.parquet'
    print('Output file name: ' + output_file)

    with timeblock('Write result to file'):
        pq.write_table(table, output_file)

    data = pq.read_table(source=output_file)

    print("Metadata from result file")
    print(data.nbytes)
    print(data.num_rows)
#    print(data.schema)
    print(data.column_names)
    pandas.set_option('max_columns', None) # print all columns
    print(data.to_pandas().head(10))
