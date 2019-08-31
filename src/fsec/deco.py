import functools


def indicate(func, indicator_name: str = None):
    """Capture the analysis method"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        obj = args[0]

        value = func(*args, **kwargs)
        setattr(obj, f"_{indicator_name or func.__name__}_indicator", True)
        return value

    return wrapper


def chained(func):
    """Add method call to the calling objects analysis chain."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        obj = args[0]
        if not hasattr(obj, "_chain"):
            obj._chain = []
        if hasattr(obj, "norm"):
            obj._chain.append((obj.norm, func.__name__))
        else:
            obj._chain.append((None, func.__name__))
        value = func(*args, **kwargs)
        if isinstance(value, pd.Series):
            if not value.empty:
                value["method"] = f"operator.classifier.{func.__name__}"
        return value

    return wrapper


class classproperty(object):
    """ Decorator to make a class-level property"""

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)
