

import functools
from collections import Counter, defaultdict
import pandas as pd

class DefaultCounter(defaultdict, Counter):
    pass

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

class classproperty(object):
    """ Decorator to make a class-level property"""
    def __init__(self, f):
        self.f = f
    def __get__(self, obj, owner):
        return self.f(owner)

def chained(func):
    """Add method call to the calling objects analysis chain."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        # print(f"Calling {func.__name__}({signature})")
        obj = args[0]
        if not hasattr(obj, '_chain'):
            obj._chain = []
        if hasattr(obj, 'norm'):
            obj._chain.append((obj.norm, func.__name__))
        else:
            obj._chain.append((None, func.__name__))
        value = func(*args, **kwargs)
        if isinstance(value, pd.Series):
            if not value.empty:
                value['method'] = f'operator.classifier.{func.__name__}'
        return value
    return wrapper

