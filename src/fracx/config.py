import sys
import logging
import os
import socket
import shutil

import tomlkit
import yaml
from attrdict import AttrDict
from sqlalchemy.engine.url import URL

_pg_aliases = ["postgres", "postgresql", "psycopg2", "psycopg2-binary"]
_mssql_aliases = ["mssql", "sql server"]

APP_SETTINGS = os.getenv("APP_SETTINGS", "fracx.config.ProductionConfig")
FLASK_APP = os.getenv("FLASK_APP", "fracx.manage.py")

sys.path.append(os.path.abspath(os.path.join("..", "config")))


try:
    __file__
except:  # noqa
    __file__ = "./src/fracx/config.py"


def abs_path(path: str, filename: str) -> str:
    return os.path.abspath(os.path.join(path, filename))


def make_config_path(path: str, filename: str) -> str:
    # return os.path.abspath(os.path.join(path, filename))
    return os.path.join(path, filename)


def safe_load_yaml(path: str) -> AttrDict:
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError as fe:
        print(f"Failed to load configuration: {fe}")


def get_active_config() -> AttrDict:
    return globals()[APP_SETTINGS.replace("fracx.config.", "")]()


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
        driver = "mssql+pymssql"

    return driver


def get_default_schema(dialect: str):
    driver = None
    if dialect in _pg_aliases:
        driver = "public"
    elif dialect in _mssql_aliases:
        driver = "dbo"

    return driver


def make_url(url_params: dict) -> URL:
    return URL(**url_params)


def _get_project_meta(pyproj_path: str = "./pyproject.toml") -> dict:
    if os.path.exists(pyproj_path):
        with open(pyproj_path, "r") as pyproject:
            file_contents = pyproject.read()
        return tomlkit.parse(file_contents)["tool"]["poetry"]
    else:
        return {}


pkg_meta = _get_project_meta()
project = pkg_meta.get("name")
version = pkg_meta.get("version")


class BaseConfig:
    """Base configuration"""

    FLASK_ENV = os.getenv("FLASK_ENV", "development")
    ENV_NAME = os.getenv("ENV_NAME", socket.gethostname())

    """ Datadog """
    DATADOG_ENABLED = os.getenv("DATADOG_ENABLED", False)
    DATADOG_API_KEY = os.getenv("DATADOG_API_KEY", None)
    DATADOG_APP_KEY = os.getenv("DATADOG_APP_KEY", None)

    """ General """
    CONFIG_BASEPATH = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data_files"
    )

    """ Collector """
    COLLECTOR_CONFIG_PATH = make_config_path(CONFIG_BASEPATH, "collector.yaml")
    COLLECTOR_CONFIG = safe_load_yaml(COLLECTOR_CONFIG_PATH)
    COLLECTOR_FTP_URL = os.getenv("FRACX_FTP_URL", "sftp.pdswdx.com")
    COLLECTOR_FTP_PORT = os.getenv("FRACX_FTP_PORT", "21")
    COLLECTOR_FTP_OUTPATH = os.getenv("FRACX_FTP_OUTPATH", "/Outbound")
    COLLECTOR_FTP_INPATH = os.getenv("FRACX_FTP_INPATH", "/Inbound")
    COLLECTOR_FTP_USERNAME = os.getenv("FRACX_FTP_USERNAME")
    COLLECTOR_FTP_PASSWORD = os.getenv("FRACX_FTP_PASSWORD")
    COLLECTOR_WRITE_SIZE = int(os.getenv("FRACX_WRITE_SIZE", "1000"))

    """ Parser """
    PARSER_CONFIG_PATH = abs_path(CONFIG_BASEPATH, "parsers.yaml")
    PARSER_CONFIG = safe_load_yaml(PARSER_CONFIG_PATH)

    """ Logging """
    LOG_LEVEL = os.getenv("FRACX_LOG_LEVEL", logging.INFO)
    LOG_FORMAT = os.getenv("FRACX_LOG_FORMAT", "layman")

    """ --------------- Sqlalchemy --------------- """
    DATABASE_DIALECT = os.getenv("FRACX_DATABASE_DIALECT", "postgres")
    DATABASE_DRIVER = os.getenv(
        "FRACX_DATABASE_DRIVER", get_default_driver(DATABASE_DIALECT)
    )
    DATABASE_USERNAME = os.getenv("FRACX_DATABASE_USERNAME", "")
    DATABASE_PASSWORD = os.getenv("FRACX_DATABASE_PASSWORD", "")
    DATABASE_HOST = os.getenv("FRACX_DATABASE_HOST", "localhost")
    DATABASE_PORT = os.getenv("FRACX_DATABASE_PORT", get_default_port(DATABASE_DRIVER))
    DATABASE_SCHEMA = os.getenv(
        "FRACX_DATABASE_SCHEMA", get_default_schema(DATABASE_DRIVER)
    )
    DATABASE_NAME = os.getenv("FRACX_DATABASE_NAME", "postgres")
    DATABASE_URL_PARAMS = {
        "drivername": DATABASE_DRIVER,
        "username": DATABASE_USERNAME,
        "password": DATABASE_PASSWORD,
        "host": DATABASE_HOST,
        "port": DATABASE_PORT,
        "database": DATABASE_NAME,
    }
    SQLALCHEMY_DATABASE_URI = str(make_url(DATABASE_URL_PARAMS))
    FRAC_SCHEDULE_TABLE_NAME = os.getenv("FRACX_TABLE_NAME", "frac_schedules")

    @property
    def show(self):
        return {
            x: getattr(self, x)
            for x in dir(self)
            if not x.startswith("_") and x.isupper()
            #  and not x.endswith("_CONFIG")
        }

    @property
    def collector_params(self):
        return self.with_prefix("collector")

    @property
    def datadog_params(self):
        return self.with_prefix("datadog")

    @property
    def endpoints(self):
        return self.COLLECTOR_CONFIG.get("endpoints")

    def __repr__(self):
        """ Print noteworthy configuration items """
        hr = "-" * shutil.get_terminal_size().columns + "\n"
        tpl = "{name:>25} {value:<50}\n"
        string = ""
        string += tpl.format(name="app config:", value=APP_SETTINGS)
        string += tpl.format(name="flask app:", value=FLASK_APP)
        string += tpl.format(name="flask env:", value=self.FLASK_ENV)
        string += tpl.format(
            name="backend:", value=make_url(self.DATABASE_URL_PARAMS).__repr__()
        )
        string += tpl.format(name="collector:", value=self.COLLECTOR_FTP_URL)
        return hr + string + hr

    def with_prefix(self, kw: str):
        """ Return all parameters that begin with the given string.

            Example: kw = "collector"

                Returns:
                    {
                        "base_url": "example.com/api",
                        "path": "path/to/data",
                        "endpoints": {...}
                    }
        """
        if not kw.endswith("_"):
            kw = kw + "_"
        return {
            key.lower().replace(kw.lower(), ""): getattr(self, key)
            for key in dir(self)
            if key.startswith(kw.upper())
        }


class DevelopmentConfig(BaseConfig):
    """Development configuration"""

    DEBUG_TB_ENABLED = True
    SECRET_KEY = os.getenv("SECRET_KEY", "test")
    SQLALCHEMY_TRACK_MODIFICATIONS = True


class TestingConfig(BaseConfig):
    """Testing configuration"""

    TESTING = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    API_BASE_URL = "https://api.example.com"

    COLLECTOR_FTP_URL = "localhost"
    COLLECTOR_FTP_PORT = "49476"
    COLLECTOR_FTP_OUTPATH = "/"
    COLLECTOR_FTP_INPATH = "/"
    COLLECTOR_FTP_USERNAME = "testuser"
    COLLECTOR_FTP_PASSWORD = "supercomplexpassword"


class CIConfig(BaseConfig):
    DATABASE_NAME = "circle_test"
    DATABASE_DRIVER = "postgres"
    DATABASE_HOST = "localhost"
    DATABASE_USERNAME = "circleci"
    DATABASE_PORT = 5432
    DATABASE_PASSWORD = ""
    DATABASE_URL_PARAMS = {
        "drivername": DATABASE_DRIVER,
        "username": DATABASE_USERNAME,
        "password": DATABASE_PASSWORD,
        "host": DATABASE_HOST,
        "port": DATABASE_PORT,
        "database": DATABASE_NAME,
    }
    SQLALCHEMY_DATABASE_URI = str(make_url(DATABASE_URL_PARAMS))


class ProductionConfig(BaseConfig):
    """Production configuration"""

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    CELERYD_PREFETCH_MULTIPLIER = 8
    CELERYD_CONCURRENCY = 12


if __name__ == "__main__":
    c = CIConfig()
