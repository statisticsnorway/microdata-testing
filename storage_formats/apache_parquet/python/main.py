import os
from datetime import date

import pyarrow.parquet as pq
import numpy as np
import fastparquet_test
import pyarrow_test
from timer import timeblock


# Get a list of all importable modules from a given path
# https://chrisyeh96.github.io/2017/08/08/definitive-guide-python-imports.html
import pkgutil
search_path = ['.']  # set to None to see all modules importable from sys.path
all_modules = [x[1] for x in pkgutil.iter_modules(path=search_path)]
print(all_modules)


def get_file_size_in_mb(file):
    return round((os.path.getsize(file) / 1000000), 3)


data_dir = '../data/'
input_file = 'DATA_50_MILLION_ROWS__1_0.parquet'
start_date = date.fromisoformat('2002-01-01')
stop_date = date.fromisoformat('2006-01-01')

print('Size of input file on disk: ' + str(get_file_size_in_mb(data_dir + input_file)) + ' MB')


# with timeblock('pyarrow run_test()'):
#     pyarrow_test.run_test(
#         input_file=data_dir + input_file,
#         output_dir=data_dir,
#         filters=[('start', '>=', start_date), ('stop', '<=', stop_date)],
#         use_pandas=True
#     )
#
# with timeblock('fastparquet run_test()'):
#     start_as_datetime64 = np.datetime64('2005-02-25')
#     stop_as_datetime64 = np.datetime64('2015-02-25')
#     fastparquet_test.run_test(
#         input_file=data_dir + input_file,
#         output_dir=data_dir,
#         filters=[('start', '>=', start_as_datetime64), ('stop', '<=', stop_as_datetime64)]
#     )
#
# with timeblock('pyarrow_test run_partition_test()'):
#     pyarrow_test.run_partition_test(
#         input_file=data_dir + 'TEST_PERSON_INCOME_1_0_for_partitioning.parquet',
#         output_dir=data_dir,
#         filters=None
#     )
#
# with timeblock('pyarrow run_id_filter_test()'):
#     pyarrow_test.run_id_filter_test(
#         input_file=data_dir + 'TEST_PERSON_INCOME_1_0_for_partitioning.parquet',
#         input_id_file=data_dir + 'TEST_PERSON_INCOME_1_0_unit_ids.parquet'
#     )
#
# with timeblock('pyarrow run_id_filter_test_dataframe_join()'):
#     pyarrow_test.run_id_filter_test_dataframe_join(
#         input_file=data_dir + 'TEST_PERSON_INCOME_1_0_for_partitioning.parquet',
#         input_id_file=data_dir + 'TEST_PERSON_INCOME_1_0_unit_ids.parquet'
#     )

with timeblock('pyarrow_test run_partition_test()'):
    output_file = pyarrow_test.run_partition_test2(
        input_file_root_path=data_dir + 'accumulated_data_300_million_rows_small_converted',
        output_dir=data_dir + 'resultsets/',
        filters=[('start_unix_days', '>=', 12000), ('stop_unix_days', '<=', 14000)]
    )
print('Parquet metadata: ' + str(pq.read_metadata(output_file)))
print('Parquet schema: ' + pq.read_schema(output_file).to_string())
print('Size of output file on disk: ' + str(get_file_size_in_mb(output_file)) + ' MB')


# TODO test nulls
# fastparquet: https://fastparquet.readthedocs.io/en/latest/details.html#nulls