import numpy
import numpy as np
import pyarrow
from pyarrow.lib import NA

import pyarrow_test
from timer import timeblock

# Get a list of all importable modules from a given path
# https://chrisyeh96.github.io/2017/08/08/definitive-guide-python-imports.html
import pkgutil
search_path = ['.']  # set to None to see all modules importable from sys.path
all_modules = [x[1] for x in pkgutil.iter_modules(path=search_path)]
print(all_modules)


root_dir = "/Users/vak/temp/data/"

with timeblock('pyarrow_test run_partition_test()'):

# 2000-01-01
# 10957
# 2000-12-31
# 11322


# 2000-06-01
# 11109
# 2002-06-01
# 11839


    start_unix_days = 11109
    stop_unix_days = 11839

    print (start_unix_days)
    print (stop_unix_days )

    missing_hack = -32768

    pyarrow_test.run_partition_test2(
        # input_file_root_path=root_dir + 'accumulated_data_300_million_rader_converted',
        # input_file_root_path=root_dir + 'y2000_only',
        input_file_root_path=root_dir + 'data_50_missing_converted',
        output_dir=root_dir + 'resultsets/',
#        filters=[('stop_unix_days', '=', NA)] # pyarrow.null() np.NaN None
#        filters=[('start_unix_days', '=', 730)]
        filters=[('start_unix_days', '=', 730), ('stop_unix_days', '=', missing_hack)]
#       filters=[('start_unix_days', '>=', start_unix_days), ('stop_unix_days', '<=', stop_unix_days)]
    )

# TODO test nulls
# fastparquet: https://fastparquet.readthedocs.io/en/latest/details.html#nulls