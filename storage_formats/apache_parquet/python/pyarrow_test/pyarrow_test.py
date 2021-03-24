import os
import sys
from time import time

import pyarrow.parquet as pq

from storage_formats.apache_parquet.python.timer import timeblock


def run_test(input_file: str, output_dir: str, filters: list):
    print('Using pyarrow')
    print('Parquet metadata: ' + str(pq.read_metadata(input_file)))
    print('Parquet schema: ' + pq.read_schema(input_file).to_string())
    print('Size of input file: ' +  str(os.path.getsize(input_file)) + ' bytes ('
          + str(os.path.getsize(input_file) / 1000000) + ' MB)')

    with timeblock('pyarrow read_table() and filter'):
        data = pq.read_table(source=input_file, filters=filters)
    size = sys.getsizeof(data)
    print('Size of filtered pyarrow table: ' + str(size) + ' bytes (' + str(size / 1000000) + ' MB)')

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + '.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        pq.write_table(data, output_file)

    print('Parquet metadata of output: ' + str(pq.read_metadata(output_file)))
    print('Parquet schema of output: ' + pq.read_schema(output_file).to_string())
    print('Size of output file: ' +  str(os.path.getsize(output_file)) + ' bytes ('
          + str(os.path.getsize(output_file) / 1000000) + ' MB)')
