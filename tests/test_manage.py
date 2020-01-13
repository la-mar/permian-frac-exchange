import pytest  # noqa
import subprocess
import logging

import sqlalchemy
from sqlalchemy import MetaData

import manage

logger = logging.getLogger(__name__)


@pytest.fixture
def table_names(conf):
    db_url = conf.SQLALCHEMY_DATABASE_URI
    engine = sqlalchemy.create_engine(db_url)

    m = MetaData()
    m.reflect(engine, views=True)

    tables = m.tables.values()

    table_names = [x.name for x in tables]
    yield table_names


class TestManage:
    def test_endpoints_command(self):
        output = subprocess.check_output(
            ["fracx", "endpoints"], universal_newlines=True
        )
        output = output.replace("\n", "")
        assert output == "frac_schedules"

    def test_run_collector_command(self, ftpserver, monkeypatch):
        login = ftpserver.get_login_data()
        url = login["host"]
        username = login["user"]
        password = login["passwd"]
        port = login["port"]
        monkeypatch.setenv("FRACX_FTP_URL", url)
        monkeypatch.setenv("FRACX_FTP_PORT", port)
        monkeypatch.setenv("FRACX_FTP_USERNAME", username)
        monkeypatch.setenv("FRACX_FTP_PASSWORD", password)
        monkeypatch.setenv("FRACX_FTP_INPATH", "/")
        monkeypatch.setenv("FRACX_FTP_OUTPATH", "/")
        subprocess.check_output(["fracx", "run", "collector"], universal_newlines=True)

    def test_db_init(self, conf):
        subprocess.run(["fracx", "db", "init"])

        db_url = conf.SQLALCHEMY_DATABASE_URI
        engine = sqlalchemy.create_engine(db_url)

        m = MetaData()
        m.reflect(engine, views=True)

        tables = m.tables.values()

        table_names = [x.name for x in tables]

        assert conf.FRAC_SCHEDULE_TABLE_NAME in table_names
        assert f"{conf.FRAC_SCHEDULE_TABLE_NAME }_most_recent_by_api10" in table_names

    def test_db_recreate(self, conf):
        subprocess.run(["fracx", "db", "recreate"])

        db_url = conf.SQLALCHEMY_DATABASE_URI
        engine = sqlalchemy.create_engine(db_url)

        m = MetaData()
        m.reflect(engine, views=True)

        tables = m.tables.values()

        table_names = [x.name for x in tables]

        assert conf.FRAC_SCHEDULE_TABLE_NAME in table_names
        assert f"{conf.FRAC_SCHEDULE_TABLE_NAME }_most_recent_by_api10" in table_names

    def test_run_cli(self):
        subprocess.run(["fracx"])

    def test_print_hr(self):
        manage.hr()
