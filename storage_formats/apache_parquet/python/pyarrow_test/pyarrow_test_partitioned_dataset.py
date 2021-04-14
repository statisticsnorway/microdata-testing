from time import time
from typing import Optional

import pandas
import pyarrow.parquet as pq
from memory_profiler import profile
from pandas import DataFrame
from pyarrow._parquet import FileMetaData
from timer import timeblock


@profile()
def run_partition_test2(input_file_root_path: str, output_dir: str, filters: Optional[list] = None) -> str:

    table = pq.read_table(source=input_file_root_path, filters=filters, use_threads=True)

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + 'run_partition_test2_result_set.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        pq.write_table(table, output_file)

    return output_file

@profile
def run_id_filter_test2(input_file_root_path: str, input_id_file: str, output_dir: str) -> str:
    with timeblock('filter ids read_table()'):
        filter_ids_table = pq.read_table(source=input_id_file)
    with timeblock('filter ids to_pandas()'):
        filter_ids_as_pandas: DataFrame = filter_ids_table.to_pandas()
    with timeblock('filter ids toList'):
        filter_ids = filter_ids_as_pandas['unit_id'].tolist()
    # filter_ids = set(filter_ids_as_pandas['unit_id'])

    print('Number of ids in filter: ' + str(len(filter_ids)))

    with timeblock('read_table() and filter'):
        table = pq.read_table(source=input_file_root_path, use_threads=True, filters=[
            ('unit_id', 'in', filter_ids)
        ])

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + 'run_id_filter_test2_result_set.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        pq.write_table(table, output_file)

    return output_file


@profile
def run_id_filter_test_dataframe_join2(input_file_root_path: str, input_id_file: str, output_dir: str) -> str:
    filter_ids_table = pq.read_table(source=input_id_file)
    filter_ids_as_pandas: DataFrame = filter_ids_table.to_pandas()

    print('Number of ids in filter: ' + str(len(filter_ids_as_pandas)))

    data_as_pandas: DataFrame = pq.read_table(source=input_file_root_path).to_pandas()

    merged: DataFrame = pandas.merge(data_as_pandas, filter_ids_as_pandas, on='unit_id', sort=False)

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + 'run_id_filter_test_dataframe_join2_result_set.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        merged.to_parquet(path=output_file, engine='pyarrow')

    return output_file
