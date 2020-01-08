from __future__ import annotations
from typing import Dict, List, Union
import logging
from pathlib import Path

from flask_sqlalchemy import Model


from api.models import *  # noqa
from collector.endpoint import Endpoint
from collector.transformer import Transformer
from config import get_active_config


logger = logging.getLogger(__name__)

conf = get_active_config()


class Collector(object):
    """ Acts as the conduit for transferring newly collected data into a backend data model """

    _tf = None
    _endpoint = None
    _functions = None
    _model = None

    def __init__(
        self,
        endpoint: Endpoint,
        functions: Dict[Union[str, None], Union[str, None]] = None,
    ):
        self.endpoint = endpoint
        self._functions = functions

    @property
    def functions(self):
        if self._functions is None:
            self._functions = conf.functions
        return self._functions

    @property
    def model(self) -> Model:
        if self._model is None:
            self._model = self.endpoint.model
        return self._model

    @property
    def tf(self):
        if self._tf is None:
            self._tf = Transformer(
                aliases=self.endpoint.mappings.get("aliases", {}),
                exclude=self.endpoint.exclude,
            )
        return self._tf

    def transform(self, data: dict) -> dict:
        return self.tf.transform(data)


class FracScheduleCollector(Collector):
    def collect(
        self,
        filelist: Union[Path, List[Path]],
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
    ):

        # do stuff here
        pass
        # self.model.core_insert(rows)


if __name__ == "__main__":
    from fsec import create_app, db

    app = create_app()
    app.app_context().push()

    # endpoints = Endpoint.load_from_config(conf)

    # endpoint = endpoints["frac_schedules"]
    # c = FracScheduleCollector(endpoint)
