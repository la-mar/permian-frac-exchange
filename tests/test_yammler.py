import logging
import pytest  # noqa
from datetime import datetime

from collector.yammler import Yammler

logger = logging.getLogger(__name__)


@pytest.fixture
def tmpyaml(tmpdir):
    path = tmpdir.mkdir("test").join("yaml.yaml")
    path.write(
        """endpoints:
            example:
                enabled: true
                model: api.models.TestModel
                ignore_unkown: true
            """
    )
    yield path


class TestYammler:
    def test_load_yaml(self, tmpyaml):

        expected = {
            "example": {
                "enabled": True,
                "model": "api.models.TestModel",
                "ignore_unkown": True,
            }
        }

        yml = Yammler(str(tmpyaml))
        assert yml.fspath == str(tmpyaml)
        assert yml["endpoints"] == expected

    def test_dump_to_file(self, tmpdir):
        path = tmpdir.mkdir("test").join("yaml.yaml")
        yml = Yammler(str(path), {"key": "value"})
        yml.dump()
        # yml2 = Yammler(str(path))
        # logger.warning(yml2)
        # assert yml2["key"] == "value"

    def test_stamp(self, tmpdir):
        path = tmpdir.mkdir("test").join("yaml.yaml")
        yml = Yammler(str(path), {})
        assert isinstance(yml.stamp(), datetime)

    def test_generic_context_manager(self, tmpyaml):
        with Yammler.context(tmpyaml) as f:
            f["tempvar"] = "tempvalue"

        # open another context to check the result was persisted
        with Yammler.context(tmpyaml) as f:
            assert f["tempvar"] == "tempvalue"
