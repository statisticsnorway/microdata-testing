import os
import sys
from time import time

import pyarrow.parquet as pq

from storage_formats.apache_parquet.python.timer import timeblock


def run_test(input_file: str, output_dir: str, filters: list, use_pandas: bool):
    print('Using pyarrow')
    print('Parquet metadata: ' + str(pq.read_metadata(input_file)))
    print('Parquet schema: ' + pq.read_schema(input_file).to_string())

    pq_file = pq.ParquetFile(input_file)
    row_group_0_metadata = pq_file.metadata.row_group(0)
    print('Parquet min for column 0, row group 0: ' + str(row_group_0_metadata.column(0).statistics.min))
    print('Parquet max for column 0, row group 0: ' + str(row_group_0_metadata.column(0).statistics.max))

    if use_pandas:
        unfiltered_pandas_data = pq.read_table(source=input_file).to_pandas()
        size = sys.getsizeof(unfiltered_pandas_data)
        print('Size of UN-filtered pandas DataFrame in memory: ' + str(size) + ' bytes (' + str(size / 1000000) + ' MB)')

    with timeblock('pyarrow read and filter'):
        data = pq.read_table(source=input_file, filters=filters)
    size = sys.getsizeof(data)
    print('Size of filtered pyarrow table in memory: ' + str(size) + ' bytes (' + str(size / 1000000) + ' MB)')

    if use_pandas:
        unfiltered_pandas_data = data.to_pandas()
        size = sys.getsizeof(unfiltered_pandas_data)
        print('Size of filtered pandas DataFrame in memory: ' + str(size) + ' bytes (' + str(size / 1000000) + ' MB)')
        # print(pandas_data.head(10))

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + '.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        pq.write_table(data, output_file)

    print('Parquet metadata of output: ' + str(pq.read_metadata(output_file)))
    print('Parquet schema of output: ' + pq.read_schema(output_file).to_string())
    print('Size of output file on disk: ' + str(os.path.getsize(output_file)) + ' bytes ('
          + str(os.path.getsize(output_file) / 1000000) + ' MB)')
