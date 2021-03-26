import os
import sys
from time import time
from fastparquet import ParquetFile
from fastparquet import write
from fastparquet.parquet_thrift.parquet.ttypes import RowGroup

from storage_formats.apache_parquet.python.timer import timeblock


def run_test(input_file: str, output_dir: str, filters: list):
    print('Using fastparquet')

    pf = ParquetFile(input_file)
    print('Parquet metadata: ' + str(pf.info))
    print('Parquet schema: ' + str(pf.schema))
    print('Parquet columns: ' + str(pf.columns))
    print('Parquet count (total number of rows): ' + str(pf.count))
    print('Parquet dtypes: ' + str(pf.dtypes))
    print('Parquet statistics: ' + str(pf.statistics))
    print('Parquet cats: ' + str(pf.cats))  # possible values of each partitioning field
    print('Parquet row_groups number: ' + str(len(pf.row_groups)))
    # print('Parquet row_groups: ' + str(pf.row_groups))

    with timeblock('fastparquet read and filter'):
        data = pf.to_pandas(filters=filters)
        # data: RowGroup = pf.filter_row_groups(filters=filters)
    # for df in pf.iter_row_groups():
    #     print(df.shape)

    size = sys.getsizeof(data)
    print('Size of filtered Pandas dataframe in memory: ' + str(size) + ' bytes (' + str(size / 1000000) + ' MB)')

    milliseconds_since_epoch = int(time() * 1000)
    output_file = output_dir + str(milliseconds_since_epoch) + '.parquet'
    print('Output file name: ' + output_file)

    with timeblock('pyarrow write_table()'):
        write(output_file, data, compression='SNAPPY')

    pf = ParquetFile(output_file)
    print('Parquet metadata of output: ' + str(str(pf.info)))
    print('Parquet schema of output: ' + str(pf.schema))
    print('Size of output file on disk: ' + str(os.path.getsize(output_file)) + ' bytes ('
          + str(os.path.getsize(output_file) / 1000000) + ' MB)')
