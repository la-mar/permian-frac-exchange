""" Various utility functions """


import os
import re
from collections import Counter, defaultdict
from stringprocessor import StringProcessor as sp

import pandas as pd


class DefaultCounter(defaultdict, Counter):
    def __gt__(self, other):
        if isinstance(other, self.__class__):
            return sum(self.values()) > sum(other.values())
        else:
            return sum(self.values()) > other


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def tokenize(
    s: str, exclude: list = None, sep: str = "_", take_basename: bool = False
) -> str:
    """Split filename into components based on the provided seperator,
    returning the nth element.

    Arguments:
        s {str} -- delimited string
        element {int} -- index of token to return from tokenized string
        exclude {list} --list of string elements to be excluded from search
        sep {str} -- delimiter used to split the input string
        basename {bool} -- call os.path.basename on the input string? Useful when s is a filepath.

    Returns:
        str -- returns nth element of tokenized string (s). Returns empty string
        if no tokens are found.
    """

    if exclude is None:
        exclude = []

    if take_basename:
        s = os.path.basename(s)

    # Split words in s
    words = re.findall(r"[\w]+", " ".join(s.split(sep)))
    words = [sp.normalize(word, lower=True) for word in words]
    words = [word for word in words if word not in exclude]

    return words
