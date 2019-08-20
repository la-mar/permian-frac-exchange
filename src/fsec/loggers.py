""" Logger definitions """

import logging
import logging.config
import os

from version import __release__

try:
    from settings import LOGLEVEL
except:
    print("Could not find LOGLEVEL in settings.")
    LOGLEVEL = logging.INFO


LOGDIR = "./log"

if not os.path.exists(LOGDIR):
    os.mkdir(LOGDIR)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[%(asctime)s - %(filename)s:%(lineno)s - %(funcName)s()] %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "funcnames": {
            "format": "[%(name)s: %(lineno)s - %(funcName)s()] %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {
            "format": "%(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "layman": {"format": "%(message)s", "datefmt": "%Y-%m-%d %H:%M:%S"},
    },
    "handlers": {
        "console": {
            "level": LOGLEVEL,
            "class": "logging.StreamHandler",
            "formatter": "layman",
        },
        "file_handler": {
            "level": LOGLEVEL,
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "funcnames",
            "filename": f"log/{__release__}.log",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 20,
        },
    },
    "root": {"level": LOGLEVEL, "handlers": ["console", "file_handler"]},
}


def standard_config():
    logging.config.dictConfig(LOGGING)
    logger = logging.getLogger()
    logger.setLevel(LOGLEVEL)
    logger.info(
        f"Configured loggers (level: {logger.level}): {[x.name for x in logger.handlers]}"
    )


if __name__ == "__main__":

    standard_config()
    logger = logging.getLogger()
    logger.error("test-message")
