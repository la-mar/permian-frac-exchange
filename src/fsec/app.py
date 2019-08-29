from typing import Generator
import logging
import os
from datetime import datetime

import yaml
from dotenv import load_dotenv

from yammler import Yammler
from util import tokenize
from stringprocessor import StringProcessor as sp
from ftp import Ftp
import loggers
import parser
from settings import (
    ALIASPATH,
    COLUMN_DTYPES,
    FSEC_DOWNLOAD_DIR,
    FSEC_OUTPUT_DIR,
    FSEC_TO_CSV,
    FSEC_TO_DATABASE,
    LOGLEVEL,
    COLUMN_NAMES,
    EXCLUDE_TOKENS,
    FSEC_FRAC_SCHEDULE_TABLE,
    DOWNLOADLOGPATH,
    LOGLEVEL,
)

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)


def remove_previous_downloads(path: str, simulate: bool = False):
    """Removes all files in the directory at the given path

    Arguments:
        path {str} -- path to directory
    """

    del_ct = 0
    for idx, fname in enumerate(os.listdir(path)):
        fpath = os.path.join(path, fname)
        if simulate:
            if idx % 2:
                yield (fname, "success", "removed")
            else:
                yield (fname, "failed", "file does note exists")
        else:
            try:
                os.remove(fpath)
                logger.debug("Removed: {0}".format(fname))
                del_ct += 1
                yield (fname, "success", "removed")
            except FileNotFoundError as fe:
                yield (fname, "warning", "file does not exist")
            except Exception as e:
                yield (fname, "error", "encountered error trying to remove file")
                logger.exception(e)

    logger.debug("Deleted {0} old files.".format(del_ct))


# def parse():

#     if os.path.isfile(ALIASPATH):
#         with open(ALIASPATH, "r") as f:
#             alias_map = yaml.safe_load(f)

#     factory = parser.Parser_Factory(
#         alias_map=alias_map, dtypes=COLUMN_DTYPES, target_colnames=COLUMN_NAMES
#     )

#     parsers = {}
#     for f in os.listdir(FSEC_DOWNLOAD_DIR):
#         f = f"{FSEC_DOWNLOAD_DIR}/{f}"
#         if os.path.isfile(f):
#             p = factory.Parser(f)
#             parsers[p.operator.name] = p

#     ps = parser.ParserCollection(parsers, name="frac_schedules")
#     ps = ps.adjust_headers()
#     ps = ps.alias_columns()
#     ps = ps.standardize()
#     ps = ps.normalize_geometry()
#     ps = ps.reshape()
#     ps.to_yaml(aliases=alias_map)
#     return ps


def load_alias_map():
    alias_map = {}
    try:
        if os.path.isfile(ALIASPATH):
            with open(ALIASPATH, "r") as f:
                alias_map = yaml.safe_load(f)

    except FileNotFoundError as fe:
        logger.warning(
            f"File containing mapped column aliases could not be found.  Expected to find this file: {os.path.abspath(ALIASPATH)}"
        )
        logger.debug(fe)

    except IOError as ioe:
        logger.warning(
            f"Encountered error when importing alias map from {os.path.abspath(ALIASPATH)}"
        )
        logger.debug(ioe)

    except Exception as e:
        logger.warning(
            f"Encountered unknown error when importing alias map from {os.path.abspath(ALIASPATH)}"
        )
        logger.debug(e)

    finally:
        return alias_map


def parse_many(dirpath: str):
    pass


def parse(filepath: str = None):

    p = parser.Parser(
        filepath,
        alias_map=load_alias_map(),
        dtypes=COLUMN_DTYPES,
        target_colnames=COLUMN_NAMES,
    )

    return p.parse()


def list_downloads(path: str):
    paths = []

    if not os.path.exists(path):
        raise FileExistsError(
            f"Cant find location: {path}. Try setting the FSEC_DOWNLOAD_DIR environment variable to the location of the downloaded frac schedules. \n\n FSEC_DOWNLOAD_DIR is currently set to {os.path.abspath(FSEC_DOWNLOAD_DIR)}"
        )

    if not os.path.isdir(path):
        raise NotADirectoryError(
            f"{path} is not a directory. Try setting the FSEC_DOWNLOAD_DIR environment variable to the location of the downloaded frac schedules. \n\n FSEC_DOWNLOAD_DIR is currently set to {os.path.abspath(FSEC_DOWNLOAD_DIR)}"
        )

    for fname in os.listdir(path):
        paths.append(os.path.join(path, fname))
    return paths


def operator_filenames(paths: list, with_index: bool = False):

    # in function to avoid trying to import if not configured
    from operator_index import Operator  # noqa

    # search in paths for resemblence to passed operator name
    ops = []
    for idx, path in enumerate(paths):
        tokens: list = tokenize(
            sp.normalize(os.path.basename(path), lower=True), sep="_"
        )
        tokens = [t for t in tokens if t not in EXCLUDE_TOKENS]
        if len(tokens):
            token = tokens[0]
        else:
            token = None

        if with_index:
            ops.append((Operator(token), idx))
        else:
            ops.append(Operator(token))

    return list(zip(paths, ops))


def to_csv(df, dirname: str = None, fileprefix: str = None):
    """Save the dataframe to the given location

    Arguments:
        df {[type]} -- data

    Keyword Arguments:
        to {str} -- file or database (default: {None})
    """

    dirname = os.path.abspath(dirname or FSEC_OUTPUT_DIR)

    if not os.path.exists(dirname):
        logger.info(f"creating directory: {dirname}")
        os.mkdir(dirname)

    to = os.path.join(dirname, f"{fileprefix}-{datetime.now().isoformat()}.csv")
    df.to_csv(to)
    return to


def to_db(df):

    # imported in function to avoid trying to import if not configured
    from tables import frame_to_db, frac_schedule  # noqa

    frame_to_db(df)
    return f"{frac_schedule.__table__.schema}.{frac_schedule.__table__.name}"


def download(path) -> Generator:
    return Ftp.load().download_all(to=path)


def upload(path):
    return Ftp.load().upload(to=path)


def run(
    from_: str = None,
    to_: str = None,
    no_cleanup: bool = False,
    no_download: bool = False,
    no_parse: bool = False,
):

    if not no_cleanup:
        remove_previous_downloads(FSEC_DOWNLOAD_DIR)

    if not no_download:
        download(FSEC_DOWNLOAD_DIR)

    if not no_parse:
        parsers = parse()
        save(parsers.df, output_to)


if __name__ == "__main__":
    run("./data")
