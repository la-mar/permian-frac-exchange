from datetime import datetime
import os
import tempfile
from contextlib import contextmanager
import logging

import yaml

logger = logging.getLogger(__name__)


class Yammler(dict):
    _no_dump = ["changed"]
    _metavars = ["fspath", "updated_at"]
    _data_key = "data"
    _meta_key = "meta"

    def __init__(self, fspath: str, data: dict = None):
        self.fspath = fspath
        self.changed = False
        self.updated_at = self.stamp()
        data = {}
        if os.path.exists(fspath):
            with open(fspath) as f:
                data = yaml.safe_load(f) or {}
        super().__init__(data)

    def dump(self) -> None:
        with self.durable(self.fspath, "w") as f:
            yaml.safe_dump(dict(self), f, default_flow_style=False)

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

