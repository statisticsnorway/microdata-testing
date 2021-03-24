from contextlib import contextmanager
import time
from functools import wraps

# Note:
# time.perf_counter() function used in the solution provides the highest-resolution timer
# possible on a given platform. However, it still measures wall-clock time, and can be
# impacted by many different factors, such as machine load.

# To time a function
def timefunc(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        r = func(*args, **kwargs)
        end = time.perf_counter()
        print('{}.{} : {}'.format(func.__module__, func.__name__, end - start))
        return r
    return wrapper

# To time a block of statements
@contextmanager
def timeblock(label):
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        print('************ {}: {}'.format(label, end - start))
