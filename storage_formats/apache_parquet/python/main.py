from datetime import date

import fastparquet_test
import pyarrow_test
from storage_formats.apache_parquet.python.timer import timeblock

data_dir = '../data/'
input_file = 'DATA_50_MILLION_ROWS__1_0.parquet'
start_date = date.fromisoformat('2002-01-01')
stop_date = date.fromisoformat('2006-01-01')

with timeblock('pyarrow run_test()'):
    pyarrow_test.run_test(
        input_file=data_dir + input_file,
        output_dir=data_dir,
        filters=[('start', '>=', start_date), ('stop', '<=', stop_date)]
    )
