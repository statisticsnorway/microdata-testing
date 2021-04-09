from time import time
from typing import Optional

import pyarrow.parquet as pq
from memory_profiler import profile
from pandas import DataFrame
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

@profile
def run_id_filter_test2(input_file_root_path: str, input_id_file: str, output_dir: str) -> str:

    # converting ids to pandas will be a "zero copy conversion" as unit_id column is int64 when:
    # - ids are not nulls
    # - a single ChunkedArray
    # TODO check it that is the case
    # https://arrow.apache.org/docs/python/pandas.html#zero-copy-series-conversions
    filter_ids_table = pq.read_table(source=input_id_file)
    filter_ids_as_pandas: DataFrame = filter_ids_table.to_pandas()
    filter_ids = filter_ids_as_pandas['unit_id'].tolist()
    # filter_ids = set(filter_ids_as_pandas['unit_id'])

    print('Number of ids in filter: ' + str(len(filter_ids)))

    table = pq.read_table(source=input_file_root_path, use_threads=False, filters=[
        ('unit_id', 'in', filter_ids)
    ])

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + 'run_id_filter_test2_result_set.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        pq.write_table(table, output_file)

    return output_file
