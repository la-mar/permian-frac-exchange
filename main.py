""" Entry point"""
import logging

import app as app
import loggers

logger = logging.getLogger(__name__)


def main():

    loggers.standard_config()

    try:
        app.run()
    except Exception as e:
        logger.exception("Process failed.")


if __name__ == "__main__":
    main()


# TODO: Check ./data folder exists
