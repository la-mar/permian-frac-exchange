from typing import Dict, List, Union, Iterable
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
    """ Conduit for transferring newly collected data into a backend data model """

    _tf = None
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
                ignore_unknown=self.endpoint.ignore_unknown,
            )
        return self._tf

    def transform(self, data: dict) -> dict:
        return self.tf.transform(data)


class FracScheduleCollector(Collector):
    def collect(
        self,
        iterable: Iterable,
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
    ):

        rows = []
        for row in iterable:
            transformed = self.filter(self.transform(row))
            if transformed:
                rows.append(transformed)

            if len(rows) > 0 and len(rows) % conf.COLLECTOR_WRITE_SIZE == 0:
                self.persist(rows)

        # persist leftovers
        self.persist(rows)

    def persist(
        self,
        rows: List[Dict],
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
    ):
        if "pymssql" in conf.DATABASE_DRIVER:
            self.model.bulk_merge(rows)
        else:
            self.model.core_insert(
                rows,
                update_on_conflict=update_on_conflict,
                ignore_on_conflict=ignore_on_conflict,
            )
        rows = []

    def filter(self, row: Dict) -> Union[Dict, None]:
        if row.get("shllat") and row.get("shllon"):
            return row
        else:
            return None


if __name__ == "__main__":
    from fracx import create_app, db
    from collector import Endpoint
    from collector.filehandler import BytesFileHandler

    logging.basicConfig(level=10)
    logger.setLevel(10)

    app = create_app()
    app.app_context().push()

    endpoints = Endpoint.load_from_config(conf)

    endpoint = endpoints["frac_schedules"]
    c = FracScheduleCollector(endpoint)

    content = b""
    with open("data/bytes.txt", "rb") as f:
        content = f.read()

    iterable = BytesFileHandler.xlsx(
        content, date_columns=endpoint.mappings.get("dates")
    )

    iterable = list(iterable)

    c.collect(iterable)

