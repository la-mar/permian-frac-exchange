import os
import sys

class DotDictMixin(dict):
    """Extension of the base dict class, adding dot.notation access
       to dictionary attributes and additional utility functions"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


    def flatten(self):
        """Flattens the contents of the dictionary to a single dimension

        Arguments:
            d {dict} -- instance or subclass of dict

        Returns:
            DotDictMixin --  flattened (1 x n dimension) dictionary
        """

        return self._flatten(self)


    def _flatten(self, d: dict):
        """Flattens the contents of the dictionary to a single dimension

        Arguments:
            d {dict} -- instance or subclass of dict

        Returns:
            DotDictMixin --  flattened (1 x n dimension) dictionary
        """

        def items():
            for key, value in d.items():
                if isinstance(value, DotDictMixin):
                    for subkey, subvalue in self._flatten(value).items():
                        yield key + "/" + subkey, subvalue
                else:
                    yield key, value

        return DotDictMixin(items())


DotDict = DotDictMixin

if __name__ == "__main__":

    d = DotDictMixin({
            'a':1,
            'b':2,
            'c':3,
            'd':4,
            'e':5,})