from typing import Union
from datetime import datetime
import os
import tempfile
from contextlib import contextmanager
import logging
from collections import Counter

import yaml

logger = logging.getLogger(__name__)


class Yammler(dict):
    _no_dump = ["changed"]
    _metavars = ["fspath", "updated_at"]

    def __init__(self, fspath: str, auto_dump: bool = False, data: dict = None):
        self.fspath = fspath
        self.auto_dump = auto_dump
        self.changed = False
        self.updated_at = self.stamp()
        _yml = None
        with open(fspath) as f:
            _yml = yaml.safe_load(f) or {}
        if isinstance(_yml, dict):
            _yml_data = _yml.pop("data", {})
        _yml_data.update(data or {})
        self._set_data(_yml_data)
        self.meta = _yml

    @property
    def meta(self) -> dict:
        meta = {}
        for mv in self._metavars:
            try:
                meta[mv] = getattr(self, mv)
            except Exception as e:
                logger.debug(e)
                meta[mv] = None
        return meta

    @meta.setter
    def meta(self, data: dict) -> None:
        data = data.pop("meta", data)  # reduce if possible
        [setattr(self, key, value) for key, value in data.items()]

    def _set_data(self, data: dict) -> None:
        super().update(data or {})

    def dump(self, force=False):
        if self.changed or force:
            with self.durable(self.fspath, "w") as f:
                d = {}
                d.update({"data": dict(self), "meta": self.meta})
                [d["data"].pop(k, None) for k in self._no_dump]
                yaml.safe_dump(d, f, default_flow_style=False)
            self.changed = False

    def updated(self):
        self.updated_at = datetime.utcnow()
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

    def overwrite(self, data: dict):
        return Yammler(self.fspath, self.auto_dump, data)

    @staticmethod
    def stamp():
        return datetime.utcnow()

    @classmethod
    @contextmanager
    def context(cls, fspath):
        obj = cls(fspath)
        try:
            yield obj
        finally:
            obj.dump(force=True)

    @classmethod
    @contextmanager
    def durable(cls, fspath: str, mode: str = "w+b"):
        """ Safely write to file """
        _fspath = fspath
        _mode = mode
        _file = tempfile.NamedTemporaryFile(_mode, delete=False)

        try:
            yield _file
        except Exception as e:  # noqa
            os.unlink(_file.name)
            raise e
        else:
            _file.close()
            os.rename(_file.name, _fspath)


class DownloadLog(Yammler):
    _special = "_known_files"

    def __init__(self, fspath: str):
        super().__init__(fspath)

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

    fspath = "./config/download_log.yaml"
    import loggers
    from yammler import Yammler

    loggers.standard_config()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    with DownloadLog.context(fspath) as f:
        # print(f)
        # f.known_files = "test"
        # print(f.known_files)
        f.add("test1")
        print(f)

    y = Yammler("./config/operators.yaml")

    s2 = [{x.pop("operator"): x} for x in s]

    from stringprocessor import StringProcessor as sp

    for s in s2:
        for k, v in s.items():
            x = s.pop(k)
            x["alias"] = sp.normalize(x.pop("alias"), lower=True)
            x["method"] = sp.normalize(x.pop("method"), lower=True)
            s[sp.normalize(k, lower=True)] = x

    for x in s2:
        for key, value in x.items():
            try:
                value["created"] = value["created"].isoformat()
                value["updated"] = value["updated"].isoformat()
            except:
                pass
            finally:
                y[key] = value

    f = DownloadLog(fspath)
    # f.known_files
    # f.add("test1")
    # f.dump()
    # f.remove("test1")

