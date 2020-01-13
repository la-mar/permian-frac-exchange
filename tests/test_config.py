import pytest  # noqa

import config


class TestConfig:
    def test_load_config_from_yaml_file_no_exists(self, capsys):
        config.safe_load_yaml("/path/that/doesnt/exist/hopefully/test.yaml")
        captured = capsys.readouterr()
        assert "Failed to load configuration" in captured.out

    def test_get_default_port_mssql(self):
        assert config.get_default_port("mssql") == 1433

    def test_get_default_driver_mssql(self):
        assert config.get_default_driver("mssql") == "mssql+pymssql"

    def test_get_default_schema_mssql(self):
        assert config.get_default_schema("mssql") == "dbo"

    def test_get_default_port_postgres(self):
        assert config.get_default_port("postgres") == 5432

    def test_get_default_driver_postgres(self):
        assert config.get_default_driver("postgres") == "postgres"

    def test_get_default_schema_postgres(self):
        assert config.get_default_schema("postgres") == "public"

    def test_get_project_meta(self):
        assert config._get_project_meta(pyproj_path="/bad/path/pyproject.toml") == {}

    def test_show_config_attrs(self):
        c = config.BaseConfig()
        assert isinstance(c.show, list)
        assert len(c.show) > 10  # arbitrary

    def test_get_collector_params(self):
        c = config.BaseConfig()
        assert isinstance(c.collector_params, dict)
