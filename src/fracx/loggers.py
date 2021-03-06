# pylint: disable=protected-access

""" Logger definitions """

import numbers
from typing import Any, Mapping, Union
import logging
import logging.config
import os
from logging import LogRecord

import json_log_formatter
import logutils.colorize


from config import get_active_config
from util.jsontools import ObjectEncoder


conf = get_active_config()


LOG_LEVELS = dict(logging._nameToLevel)
LOG_LEVELS.update(logging._levelToName)  # type: ignore
LOG_LEVELS.update({str(k): v for k, v in logging._levelToName.items()})  # type: ignore
LOG_LEVELS.setdefault("FATAL", logging.FATAL)
LOG_LEVELS.setdefault(logging.FATAL, "FATAL")  # type: ignore

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("zeep").setLevel(logging.CRITICAL)
logging.getLogger("requests_oauthlib").setLevel(logging.CRITICAL)
logging.getLogger("sqlalchemy").setLevel(logging.CRITICAL)


def mlevel(level):
    """Convert level name/int to log level. Borrowed from celery, with <3"""
    if level and not isinstance(level, numbers.Integral):
        return LOG_LEVELS[level.upper()]
    return level


class ColorizingStreamHandler(logutils.colorize.ColorizingStreamHandler):
    """
    A stream handler which supports colorizing of console streams
    under Windows, Linux and Mac OS X.

    :param strm: The stream to colorize - typically ``sys.stdout``
                 or ``sys.stderr``.
    """

    # color names to indices
    color_map = {
        "black": 0,
        "red": 1,
        "green": 2,
        "yellow": 3,
        "blue": 4,
        "magenta": 5,
        "cyan": 6,
        "white": 7,
    }

    # levels to (background, foreground, bold/intense)
    if os.name == "nt":
        level_map = {
            logging.DEBUG: (None, "blue", True),
            logging.INFO: (None, "white", False),
            logging.WARNING: (None, "yellow", True),
            logging.ERROR: (None, "red", True),
            logging.CRITICAL: ("red", "white", True),
        }
    else:
        "Maps levels to colour/intensity settings."
        level_map = {
            logging.DEBUG: (None, "blue", False),
            logging.INFO: (None, "white", False),
            logging.WARNING: (None, "yellow", False),
            logging.ERROR: (None, "red", False),
            logging.CRITICAL: ("red", "white", True),
        }


class DatadogJSONFormatter(json_log_formatter.JSONFormatter):
    """JSON log formatter for logs sent to Datadog"""

    def format(self, record: LogRecord):
        """Return the record in a format usable by Datadog."""
        json_record = self.json_record(record.getMessage(), record)
        mutated_record = self.mutate_json_record(json_record)

        if mutated_record is None:
            mutated_record = json_record
        return self.to_json(mutated_record)

    def to_json(self, record: Mapping[str, Any]):
        """Convert record dict to a JSON string.
        Override this method to change the way dict is converted to JSON.
        """
        return self.json_lib.dumps(record, cls=ObjectEncoder)

    def json_record(self, message: str, record: LogRecord):
        """Convert the record to JSON and inject Datadog attributes."""
        record_dict = dict(record.__dict__)

        record_dict["message"] = message

        if "timestamp" not in record_dict:
            # UNIX time in milliseconds
            record_dict["timestamp"] = int(record.created * 1000)

        if "severity" not in record_dict:
            record_dict["severity"] = record.levelname

        if "logger.name" not in record_dict:
            record_dict["logger.name"] = record.name
        if "logger.method_name" not in record_dict:
            record_dict["logger.method_name"] = record.funcName
        if "logger.thread_name" not in record_dict:
            record_dict["logger.thread_name"] = record.threadName

        exc_info = record.exc_info

        if exc_info:
            if "error.kind" not in record_dict:
                record_dict["error.kind"] = exc_info[0].__name__  # type: ignore
            if "error.message" not in record_dict:
                record_dict["error.message"] = str(exc_info[1])
            if "error.stack" not in record_dict:
                record_dict["error.stack"] = self.formatException(exc_info)

        return record_dict


def get_formatter(name: Union[str, None]) -> logging.Formatter:
    formatters = {
        "verbose": logging.Formatter(
            fmt="[%(asctime)s - %(filename)s:%(lineno)s - %(funcName)s()] %(levelname)s - %(message)s",  # noqa
            datefmt="%Y-%m-%d %H:%M:%S",
        ),
        "funcname": logging.Formatter(
            fmt="[%(name)s: %(lineno)s - %(funcName)s()] %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ),
        "simple": logging.Formatter(
            fmt="%(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        ),
        "layman": logging.Formatter(fmt="%(message)s", datefmt="%Y-%m-%d %H:%M:%S"),
        "json": DatadogJSONFormatter(),
    }
    return formatters[name or "funcname"]  # type: ignore


def config(level: int = None, formatter: str = None, logger: logging.Logger = None):

    root_logger = logger or logging.getLogger()
    root_logger.setLevel(mlevel(conf.LOG_LEVEL) or level or 20)
    console_handler = ColorizingStreamHandler()
    console_handler.setFormatter(get_formatter(formatter or conf.LOG_FORMAT))
    if root_logger.handlers:
        root_logger.removeHandler(root_logger.handlers[0])
    root_logger.addHandler(console_handler)


if __name__ == "__main__":

    config(formatter="json")
    logger = logging.getLogger()
    logger.debug("test-debug")
    logger.info("test-info")
    logger.warning("test-warning")
    logger.error("test-error")
