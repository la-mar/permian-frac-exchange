""" Settings module """

import logging
import os
from calendar import month_name
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()


""" Optional Pandas display settings"""
pd.options.display.max_rows = None
pd.set_option("display.float_format", lambda x: "%.2f" % x)
pd.set_option("large_repr", "truncate")
pd.set_option("precision", 2)


ENV_NAME = os.getenv("ENV_NAME", "UNKNOWN")

""" Inputs """


""" Outputs """
FSEC_TO_CSV = os.getenv("FSEC_TO_CSV", True)
FSEC_TO_DATABASE = os.getenv("FSEC_TO_DATABASE", False)
FSEC_AUTO_MERGE = os.getenv("FSEC_AUTO_MERGE", True)


""" API keys """
SENTRY_KEY = os.getenv("FSEC_SENTRY_KEY", None)
SENTRY_LEVEL = logging.WARNING
SENTRY_EVENT_LEVEL = logging.WARNING


""" FTP Parameters """
FSEC_FTP_URL = os.getenv("FSEC_FTP_URL")
FSEC_FTP_USERNAME = os.getenv("FSEC_FTP_USERNAME")
FSEC_FTP_PASSWORD = os.getenv("FSEC_FTP_PASSWORD")
FSEC_FTP_PATH = os.getenv("FSEC_FTP_PATH")

""" Data Path """
FSEC_DOWNLOAD_DIR = os.getenv("FSEC_DOWNLOAD_DIR", "./data")

""" Output Path """
FSEC_OUTPUT_DIR = os.getenv("FSEC_OUTPUT_DIR", "./output")

""" Config Paths """
CONFIG_BASEPATH = "./config"


def config_path(filename: str):
    return os.path.abspath(os.path.join(CONFIG_BASEPATH, filename))


ALIASPATH = config_path("column_aliases.yaml")
UNKNOWNPATH = config_path("unknown_names.yaml")
CONFIGPATH = config_path("config.yaml")
OPERATORPATH = config_path("operators.json")
DOWNLOADLOGPATH = config_path("download_log.yaml")


""" Logging """
LOG_CONFIG_PATH = config_path("logging.yaml")
LOGLEVEL = os.getenv("FSEC_LOGLEVEL", logging.INFO)
LOGDIR = "./log"

""" --------------- Sqlalchemy --------------- """

""" Database connection details

        Template: dialect+driver://username:password@host:port/database

        Ex.
            MSSQL: 'mssql+pymssql://SERVER_NAME/DATABASE_NAME'


 """

_pg_aliases = ["postgres", "postgresql", "psychopg2", "psychopg2-binary"]
_mssql_aliases = ["mssql", "sql server"]


def get_default_port(driver: str):
    port = None
    if driver in _pg_aliases:
        port = 5432
    elif driver in _mssql_aliases:
        port = 1433

    return port


def get_default_driver(dialect: str):
    driver = None
    if dialect in _pg_aliases:
        driver = "postgres"  # "psycopg2"
    elif dialect in _mssql_aliases:
        driver = "pymssql"

    return driver


def get_default_schema(dialect: str):
    driver = None
    if dialect in _pg_aliases:
        driver = "public"
    elif dialect in _mssql_aliases:
        driver = "dbo"

    return driver


REGISTRY_BACKEND = os.getenv("FSEC_REGISTRY_BACKEND", "file")

DATABASE_DIALECT = os.getenv("FSEC_DATABASE_DIALECT", "postgres")
DATABASE_DRIVER = os.getenv(
    "FSEC_DATABASE_DRIVER", get_default_driver(DATABASE_DIALECT)
)
DATABASE_USERNAME = os.getenv("FSEC_DATABASE_USERNAME", "")
DATABASE_PASSWORD = os.getenv("FSEC_DATABASE_PASSWORD", "")
DATABASE_HOST = os.getenv("FSEC_DATABASE_HOST", "localhost")
DATABASE_PORT = os.getenv("FSEC_DATABASE_PORT", get_default_port(DATABASE_DRIVER))
DATABASE_SCHEMA = os.getenv("FSEC_DATABASE_SCHEMA", get_default_schema(DATABASE_DRIVER))
DATABASE_NAME = os.getenv("FSEC_DATABASE_NAME", "postgres")
DATABASE_URL_PARAMS = {
    "drivername": DATABASE_DRIVER,
    "username": DATABASE_USERNAME,
    "password": DATABASE_PASSWORD,
    "host": DATABASE_HOST,
    "port": DATABASE_PORT,
    "database": DATABASE_NAME,
}

OPERATOR_TABLENAME = os.getenv("FSEC_OPERATOR_TABLENAME", "operator")
FRAC_SCHEDULE_TABLENAME = os.getenv("FSEC_FRAC_SCHEDULE_TABLENAME", "FracSchedule")

CREATED_QUALIFIER = os.getenv("FSEC_CREATED_QUALIFIER", "created")
UPDATED_QUALIFIER = os.getenv("FSEC_UPDATED_QUALIFIER", "updated")

LOAD_GEOMETRY = False

""" Exclude these fields from ORM operations since they are managed by the database. """
EXCLUSIONS = [
    "updated",
    "inserted",
    "id",
    "days_to_fracstartdate",
    "days_to_fracenddate",
    "status",
]

""" Parser filename exclusions:
        Tokens in this list will be ignored when parsing file name.
        If the parser class is not finding the correct operator name in a file's name,
        adding elements to this list can help the parser find the correct name."""
EXCLUDE_TOKENS = list(
    map(
        str.lower,
        ["mb", "pb", "frac", "schedule", "3monthfracschedule"] + list(month_name),
    )
)

""" Mapping of column names to the desired datatype. Pandas will cast columns in
    FracSchedule dataframes into these data types."""
COLUMN_DTYPES = {
    "operator": str,
    "operator_alias": str,
    "wellname": str,
    "api14": str,
    "api10": str,
    "fracstartdate": datetime,
    "fracenddate": datetime,
    "shllat": float,
    "shllon": float,
    "bhllat": float,
    "bhllon": float,
    "tvd": float,
    "crs": str,
    "geometry": object,
}

""" Column names of a DataFrame containing FracSchedules """
COLUMN_NAMES = list(COLUMN_DTYPES.keys())

""" Exception Message Templates"""
MSG_PARSER_CHECK = "{p} -- skipped column {col_name}"
MSG_PARSER_CORRUPT = "{p} is corrupt - bypassing method call."
MSG_PARSER_ERROR = "Error in {p} -- {e}"
MSG_PARSER_PARSING = "Error parsing value ({v}) in Parser({op_name}) -- {e}"
MSG_PARSER_COLLECTION_STATUS = "Executing {func_name} on ParserCollection({pc_name})"


""" --------------- Geographic Constants --------------- """

""" EPSG code for WGS84 """
WGS84 = 4326

""" EPSG code for NAD27 """
NAD27 = 4267

""" EPSG code for NAD27 State Plane projection """
NAD27SP = 32039
