import pytest

import logging
from collector import Endpoint
from collector.downloader import Ftp

from fracx import create_app
from config import TestingConfig

logger = logging.getLogger(__name__)


@pytest.fixture
def conf():
    conf = TestingConfig()
    conf.SQLALCHEMY_DATABASE_URI = "postgres://localhost:5432/postgres"
    yield conf


@pytest.fixture
def app():
    app = create_app()
    return app


@pytest.fixture
def endpoint(conf):
    yield Endpoint(name="ep_test", model="api.models.FracSchedule")


@pytest.fixture
def ftp(ftpserver):
    login = ftpserver.get_login_data()
    url = login["host"]
    username = login["user"]
    password = login["passwd"]
    port = login["port"]

    ftp = Ftp(url, username, password, port=port)
    yield ftp

    # close the ftp session
    try:
        if ftp.sock is not None:
            try:
                ftp.quit()
            except Exception as e:
                logger.error(f"Encountered error closing FTP session -- {e}")
    finally:
        ftp.close()


# @pytest.fixture
# def collector(endpoint):
#     yield FracScheduleCollector(endpoint)
