""" Entry point"""
import logging

import src.loggers
import src.app as app

logger = logging.getLogger(__name__)

def main():

    src.loggers.standard_config()

    try:
        app.run()
    except Exception as e:
        logger.exception('Process failed.')

if __name__ == "__main__":
    main()




