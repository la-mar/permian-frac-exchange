from typing import Callable, Union, Iterable, Generator, Dict
import math
import json
import urllib.parse
import itertools

from util.strings import StringProcessor  # noqa
from util.jsontools import DateTimeEncoder


def hf_size(size_bytes: Union[str, int]) -> str:
    """Human friendly string representation of a size in bytes.

    Source: https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python

    Arguments:
        size_bytes {Union[str, int]} -- size of object in number of bytes

    Returns:
        str -- string representation of object size. Ex: 299553704 -> "285.68 MB"
    """
    if size_bytes == 0:
        return "0B"

    suffixes = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

    if isinstance(size_bytes, str):
        size_bytes = int(size_bytes)

    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {suffixes[i]}"


def chunks(iterable: Iterable, n: int = 1000) -> Generator:
    """ Process an interable in chunks of size n (default=1000) """
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


def apply_transformation(
    data: dict, convert: Callable, keys: bool = False, values: bool = True
) -> Dict:
    """ Recursively apply the passed function to a dict's keys, values, or both """
    if isinstance(data, (str, int, float)):
        if values:
            return convert(data)
        else:
            return data
    if isinstance(data, dict):
        new = data.__class__()
        for k, v in data.items():
            if keys:
                new[convert(k)] = apply_transformation(v, convert, keys, values)
            else:
                new[k] = apply_transformation(v, convert, keys, values)
    elif isinstance(data, (list, set, tuple)):
        new = data.__class__(
            apply_transformation(v, convert, keys, values) for v in data
        )
    else:
        return data
    return new


def urljoin(base: str = "", path: str = "") -> str:
    base = base or ""
    path = path or ""
    if not base.endswith("/"):
        base = base + "/"
    if path.startswith("/"):
        path = path[1:]
    return urllib.parse.urljoin(base, path)


def to_json(d: dict, path: str, cls=DateTimeEncoder):
    with open(path, "w") as f:
        json.dump(d, f, cls=cls, indent=4)


def load_json(path: str) -> Dict:
    with open(path, "r") as f:
        return json.load(f)


def query_dict(path: str, data: dict, sep: str = ".") -> Union[Dict, None]:
    elements = path.split(sep)
    for e in elements:
        if not issubclass(type(data), dict):
            raise ValueError(f"{data} ({type(data)}) is not a subclass of dict")
        data = data.get(e, {})
    return data if data != {} else None
