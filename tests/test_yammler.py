import logging
import pytest  # noqa
from datetime import datetime

from collector.yammler import Yammler

logger = logging.getLogger(__name__)


class TestYammler:
    def test_load_yaml(self, tmpdir):
        path = tmpdir.mkdir("test").join("yaml.yaml")
        path.write(
            """endpoints:
            frac_schedules:
                enabled: true
                model: api.models.FracSchedule
                ignore_unkown: true
            """
        )

        expected = {
            "frac_schedules": {
                "enabled": True,
                "model": "api.models.FracSchedule",
                "ignore_unkown": True,
            }
        }

        yml = Yammler(str(path))
        assert yml.fspath == str(path)
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

