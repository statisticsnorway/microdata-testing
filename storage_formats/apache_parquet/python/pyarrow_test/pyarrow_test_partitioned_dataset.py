from time import time
from typing import Optional

import pyarrow.parquet as pq
from memory_profiler import profile
from pyarrow._parquet import FileMetaData
from timer import timeblock


# @profile()
def run_partition_test2(input_file_root_path: str, output_dir: str, filters: Optional[list] = None):

    # first_file = input_file_root_path + '/start_year=1972/a2a0bc3c27c84d27842694dc622761a1.parquet'
    common_metadata_file = input_file_root_path + '/_common_metadata'
    print('Parquet metadata: ' + str(pq.read_metadata(common_metadata_file)))
    print('Parquet schema: ' + pq.read_schema(common_metadata_file).to_string())

    # help(FileMetaData)
    # data_schema = pa.schema([
    #     ('unit_id', pa.uint64()),
    #     ('value', pa.string()),
    #     ('start', pa.uint32()),
    #     ('stop', pa.uint32()),
    #     ('start_year', pa.uint16()),
    #     ('start_unix_days', pa.uint16()),  # more than AD 2100
    #     ('stop_unix_days', pa.uint16()),
    # ])
    #
    # pq_file = pq.ParquetFile(input_file)

    #FileMetaData.set_file_path(input_file_root_path + '_common_metadata')
    file_metadata = FileMetaData()

    table = pq.read_table(source=input_file_root_path, filters=filters, use_threads=False,
                          metadata=file_metadata)

    # print(table.to_pandas())

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + 'run_partition_test2_result_set.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        pq.write_table(table, output_file)
