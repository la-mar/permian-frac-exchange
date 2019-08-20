import logging
import os
from datetime import datetime

import yaml
from dotenv import load_dotenv

from ftp import Ftp
import loggers
import parser
from settings import (
    ALIASPATH,
    COLUMN_DTYPES,
    DATAPATH,
    TO_CSV,
    TO_DATABASE,
    LOGLEVEL,
    COLUMN_NAMES,
    CSV_OUTPUT_PATH_TEMPLATE,
)

load_dotenv()

loggers.standard_config()
logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)


def remove_previous_downloads(path: str):
    """Removes all files in the directory at the given path

    Arguments:
        path {str} -- path to directory
    """

    del_ct = 0
    for fname in os.listdir(path):
        fpath = os.path.join(path, fname)
        try:
            os.remove(fpath)
            logger.info("Removed: {0}".format(fname))
            del_ct += 1
        except Exception as e:
            logger.exception(e)

    logger.info("Deleted {0} old files.".format(del_ct))


# def parse():

#     if os.path.isfile(ALIASPATH):
#         with open(ALIASPATH, "r") as f:
#             alias_map = yaml.safe_load(f)

#     factory = parser.Parser_Factory(
#         alias_map=alias_map, dtypes=COLUMN_DTYPES, target_colnames=COLUMN_NAMES
#     )

#     parsers = {}
#     for f in os.listdir(DATAPATH):
#         f = f"{DATAPATH}/{f}"
#         if os.path.isfile(f):
#             p = factory.Parser(f)
#             parsers[p.operator.name] = p

#     ps = parser.ParserCollection(parsers, name="frac_schedules")
#     ps = ps.adjust_headers()
#     ps = ps.alias_columns()
#     ps = ps.standardize_data()
#     ps = ps.normalize_geometry()
#     ps = ps.reshape()
#     ps.to_yaml(aliases=alias_map)
#     return ps


def parse(path: str = None):
    if os.path.isfile(ALIASPATH):
        with open(ALIASPATH, "r") as f:
            alias_map = yaml.safe_load(f)

    factory = parser.Parser_Factory(
        alias_map=alias_map, dtypes=COLUMN_DTYPES, target_colnames=COLUMN_NAMES
    )

    parsers = {}

    if path is None:
        path = DATAPATH

    if os.path.isdir(path):
        paths = os.listdir(path)

    else:  # is file
        paths = [path]
        path = os.path.basename(path)
    for f in paths:
        f = f"{path}/{f}"
        if os.path.isfile(f):
            p = factory.Parser(f)
            parsers[p.operator.name] = p

    ps = parser.ParserCollection(parsers, name="frac_schedules")
    ps = ps.adjust_headers()
    ps = ps.alias_columns()
    ps = ps.standardize_data()
    ps = ps.normalize_geometry()
    ps = ps.reshape()
    ps.to_yaml(aliases=alias_map)


def list_downloads(path: str):
    paths = []

    if not os.path.exists(path):
        raise FileExistsError(
            f"Cant find location: {path}. Try setting the FSEC_DESTINATION environment variable to the location of the downloaded frac schedules. \n\n FSEC_DESTINATION is currently set to {os.path.abspath(DATAPATH)}"
        )

    if not os.path.isdir(path):
        raise NotADirectoryError(
            f"{path} is not a directory. Try setting the FSEC_DESTINATION environment variable to the location of the downloaded frac schedules. \n\n FSEC_DESTINATION is currently set to {os.path.abspath(DATAPATH)}"
        )

    for fname in os.listdir(path):
        paths.append(os.path.join(path, fname))
    return paths


def save(df, void: str = None):
    """Save the passed dataframe

    Arguments:
        df {[type]} -- [description]

    Keyword Arguments:
        destination {str} -- [description] (default: {None})
    """

    if not os.path.exists("./output"):
        os.mkdir("./output")

    if TO_CSV:
        destination = CSV_OUTPUT_PATH_TEMPLATE.format(
            dt=int(datetime.now().timestamp())
        )
        df.to_csv(destination)
        logger.info(f"Saved output to: {destination}")

    if TO_DATABASE:
        from tables import frame_to_db, frac_schedule

        frame_to_db(df)
        logger.info(
            f"Saved output to: {frac_schedule.__table__.schema}.{frac_schedule.__table__.name}"
        )


def run(
    destination: str = None, skip_cleanup: bool = False, skip_download: bool = False
):

    if not skip_cleanup:
        remove_previous_downloads(DATAPATH)

    if not skip_download:
        Ftp.load(None).download_all()

    parsers = parse()

    save(parsers.df, destination)


if __name__ == "__main__":
    run("./data")
