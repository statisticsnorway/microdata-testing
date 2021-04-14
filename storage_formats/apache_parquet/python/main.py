import os
from datetime import date

import pyarrow.parquet as pq
import numpy as np
from pyarrow._dataset import Expression

import fastparquet_test
import pyarrow_test
from timer import timeblock
import pyarrow as pa
import pyarrow.dataset

# Get a list of all importable modules from a given path
# https://chrisyeh96.github.io/2017/08/08/definitive-guide-python-imports.html
import pkgutil
search_path = ['.']  # set to None to see all modules importable from sys.path
all_modules = [x[1] for x in pkgutil.iter_modules(path=search_path)]
print(all_modules)


def get_file_size_in_mb(file):
    return round((os.path.getsize(file) / 1000000), 3)


def print_statistics(file):
    print('Parquet metadata: ' + str(pq.read_metadata(file)))
    print('Parquet schema: ' + pq.read_schema(file).to_string())
    print('Size of output file on disk: ' + str(get_file_size_in_mb(file)) + ' MB')
    print('Data:')
    print(pq.read_table(file).to_pandas().head())

data_dir = '../data/'


# with timeblock('pyarrow run_test()'):
#     start_date = date.fromisoformat('2002-01-01')
#     stop_date = date.fromisoformat('2006-01-01')
#     input_file = 'DATA_50_MILLION_ROWS__1_0.parquet'
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
#     input_file = 'DATA_50_MILLION_ROWS__1_0.parquet'
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

ex: Expression = pyarrow.dataset.field("start_unix_days").isin([730])
ex2: Expression = ~pyarrow.dataset.field("stop_unix_days").is_valid()

with timeblock('pyarrow_test run_partition_test()'):
    output_file = pyarrow_test.run_partition_test2(
        input_file_root_path=data_dir + 'accumulated_data_300_million_rows_small_converted',
        output_dir=data_dir + 'resultsets/',

        # filters=~pyarrow.dataset.field("stop_unix_days").is_valid()

        # filters=pyarrow.dataset.field("start_unix_days").isin([730])

        # filters=pyarrow.dataset.field("start_unix_days").isin([730, 17167])

        # filters=ex.__or__(ex2)

        #filters=ex.__and__(ex2)

        filters=ex | ex2
    )
print_statistics(output_file)


# with timeblock('pyarrow_test run_id_filter_test2()'):
#     output_file = pyarrow_test.run_id_filter_test2(
#         input_file_root_path=data_dir + 'accumulated_data_300_million_rows_converted',
#         input_id_file=data_dir + 'accumulated_data_300_million_rows_id_filter_1mill.parquet',
#         output_dir=data_dir + 'resultsets/'
#     )
# print_statistics(output_file)


# with timeblock('pyarrow_test run_id_filter_test2()'):
#     output_file = pyarrow_test.run_id_filter_test_dataframe_join2(
#         input_file_root_path=data_dir + 'accumulated_data_300_million_rows_converted',
#         input_id_file=data_dir + 'accumulated_data_300_million_rows_id_filter_1mill.parquet',
#         output_dir=data_dir + 'resultsets/'
#     )
# print_statistics(output_file)
