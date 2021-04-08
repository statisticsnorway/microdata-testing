from time import time
from typing import Optional

import pyarrow.parquet as pq
from memory_profiler import profile
from pyarrow._parquet import FileMetaData
from timer import timeblock


@profile()
def run_partition_test2(input_file_root_path: str, output_dir: str, filters: Optional[list] = None) -> str:

    table = pq.read_table(source=input_file_root_path, filters=filters, use_threads=False)

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + 'run_partition_test2_result_set.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        pq.write_table(table, output_file)

    return output_file
