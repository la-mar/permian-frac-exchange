from typing import Union
from datetime import datetime
import os
from contextlib import contextmanager
import logging
from collections import Counter

import yaml
from attrdict import AttrDict

logger = logging.getLogger(__name__)


class Yammler(dict):
    _no_dump = ["changed"]
    _metavars = ["filename", "updated_at"]

    def __init__(self, filename: str, auto_dump: bool = False):
        self.filename = filename
        self.auto_dump = auto_dump
        self.changed = False
        self.updated_at = datetime.now()
        if os.path.isfile(filename):
            with open(filename) as f:
                # use super here to avoid unnecessary write
                # super().update(yaml.safe_load(f) or {})
                super().update(yaml.safe_load(f) or {})

    def _meta(self):
        return {x: getattr(self, x) for x in self._metavars}

    def dump(self, force=False):
        if self.changed or force:
            with open(self.filename, "w") as f:
                d = dict(self)
                d.update(self._meta())
                [d.pop(k, None) for k in self._no_dump]
                yaml.safe_dump(d, f, default_flow_style=False)
            self.changed = False

    def updated(self):

        self.updated_at = datetime.now()

        if self.auto_dump:
            self.dump(force=True)
        else:
            self.changed = True

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.updated()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.updated()

    def update(self, kwargs):
        super().update(kwargs)
        self.updated()

    @classmethod
    @contextmanager
    def context(cls, filename):
        obj = cls(filename)
        try:
            yield obj
        finally:
            obj.dump(force=True)


class DownloadLog(Yammler):
    _special = "_known_files"

    def __init__(self, filename: str):
        super().__init__(filename)

    def __repr__(self):
        return f"DownloadLog: {len(self.known_files)} tracked files"

    def __iter__(self):
        for f in self.known_files:
            yield f

    def __len__(self):
        return len(self.__getitem__(self._special))

    def __missing__(self, key):
        if key == self._special:
            value = dict()
            self[key] = value
            return value
        else:
            raise KeyError

    @property
    def known_files(self) -> dict:
        return self.__getitem__(self._special)

    def add(self, paths: Union[str, list]) -> Counter:
        if not isinstance(paths, list):
            paths = [paths]

        c: Counter = Counter()
        for path in paths:
            try:
                self.known_files[path] = datetime.now().isoformat()
                c["ok"] += 1
            except ValueError as ve:
                c["failed"] += 1
                logger.info(f"Failed to add entry to {self.__class__.__name__}: {path}")
                raise ve

        return c

    def remove(self, paths: Union[str, list]) -> Counter:
        if not isinstance(paths, list):
            paths = [paths]

        c: Counter = Counter()
        for path in paths:
            try:
                self.known_files.pop(path)
                c["ok"] += 1
            except ValueError:
                c["failed"] += 1
                logger.debug(f"{path} not in {self.__class__.__name__}")

        logger.debug(c)
        return c

    # def cycle(self, items: Union[str, list], max_age: int = 42):
    #     """ Purge log older than max_age (days)"""

    #     self.remove(items)
    #     self.purged_at = datetime.now()
    #     return self


if __name__ == "__main__":

    filename = "./config/download_log.yaml"
    import loggers

    loggers.standard_config()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    with DownloadLog.context(filename) as f:
        # print(f)
        # f.known_files = "test"
        # print(f.known_files)
        f.add("test1")
        print(f)

    y = Yammler(filename)

    f = DownloadLog(filename)
    # f.known_files
    # f.add("test1")
    # f.dump()
    # f.remove("test1")

