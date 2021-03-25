import os
import sys
from time import time
from fastparquet import ParquetFile
from fastparquet import write

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

    # pf.row_groups
    # pf.filter_row_groups()

    with timeblock('fastparquet read and filter'):
        data = pf.to_pandas(filters=filters)
    size = sys.getsizeof(data)
    print('Size of filtered Pandas dataframe in memory: ' + str(size) + ' bytes (' + str(size / 1000000) + ' MB)')

    # need a DataFrame
    # write('outfile.parq', df, compression='SNAPPY')
