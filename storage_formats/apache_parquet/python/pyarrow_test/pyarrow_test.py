import os
import sys
from datetime import date
from time import time
from typing import Optional

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


def run_partition_test(input_file: str, output_dir: str, filters: Optional[list] = None):
    milliseconds_since_epoch = int(time() * 1000)

    data = pq.read_table(source=input_file, filters=filters)

    # Write a dataset and collect metadata information of all written files
    metadata_collector = []
    root_path = output_dir + 'partitioned_' + str(milliseconds_since_epoch)
    pq.write_to_dataset(data,
                        root_path=root_path,
                        partition_cols=['start_year'],
                        metadata_collector=metadata_collector)

    # Write the ``_common_metadata`` parquet file without row groups statistics
    pq.write_metadata(data.schema, root_path + '/_common_metadata')

    # Write the ``_metadata`` parquet file with row groups statistics of all files
    # Gives following error:
    #       File "pyarrow/_parquet.pyx", line 616, in pyarrow._parquet.FileMetaData.append_row_groups
    #       RuntimeError: AppendRowGroups requires equal schemas.
    # data.schema has one more column than partitioned files when partitioning by one column
    # Related? https://github.com/dask/dask/issues/6243
    # pq.write_metadata(data.schema, root_path + '/_metadata', metadata_collector=metadata_collector)

    # Read from partitioned dataset
    # use the new generic Dataset API
    start_year = 2018
    value = 50000
    table = pq.read_table(root_path,
                          filters=[('start_year', '>=', start_year), ('value', '>', value)])
                          # filters=[('start_year', '>=', start_year)])
    print(table.to_pandas())
