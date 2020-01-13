import logging
import os
import pytest  # noqa

from loggers import (
    mlevel,
    ColorizingStreamHandler,
    DatadogJSONFormatter,
    get_formatter,
    config,
)


@pytest.fixture
def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    yield logger


class TestLogger:
    def test_logger(self, caplog, logger):
        logger.info("test")
        assert "test" in caplog.text

    def test_convert_qualifier_to_log_level(self, logger):
        assert mlevel("debug") == 10
        assert mlevel("info") == 20
        assert mlevel("warning") == 30
        assert mlevel("error") == 40
        assert mlevel("critical") == 50

    def test_colorized_stream_handler_nt(self, monkeypatch, logger, caplog):

        handler = ColorizingStreamHandler()
        logger.addHandler(handler)
        monkeypatch.setattr(os, "name", "nt")

        logger.info("test nt colorized")

        assert "test nt colorized" in caplog.text

    def test_datadog_json_formatter(self, logger, caplog):
        fmt = get_formatter("json")
        handler = ColorizingStreamHandler()
        handler.setFormatter(fmt)
        if logger.handlers:
            logger.removeHandler(logger.handlers[0])
        logger.addHandler(handler)

        logger.info("json message")

    def test_datadog_json_formatter_tracing(self, logger, caplog):
        fmt = DatadogJSONFormatter()
        fmt.trace_enabled = True
        handler = ColorizingStreamHandler()
        handler.setFormatter(fmt)
        if logger.handlers:
            logger.removeHandler(logger.handlers[0])
        logger.addHandler(handler)

        logger.info("traced message")

