import pytest
from datetime import datetime, timedelta
import json

from util.jsontools import ObjectEncoder, DateTimeEncoder


@pytest.fixture
def kv():
    yield {"key": datetime.utcfromtimestamp(0)}


@pytest.fixture
def datetime_encoder():
    yield DateTimeEncoder()


class TestDatetimeEncoder:
    def test_encode_datetime(self, datetime_encoder):
        data = {"key": datetime.utcfromtimestamp(0)}
        expected = '{"key": "1970-01-01T00:00:00"}'
        result = datetime_encoder.encode(data)
        assert result == expected

    def test_encode_non_datetime(self, datetime_encoder):
        data = {"key": "test123", "key2": "test234"}
        expected = '{"key": "test123", "key2": "test234"}'
        result = datetime_encoder.encode(data)
        assert result == expected

    def test_dump_datetime(self):
        data = {"key": datetime.utcfromtimestamp(0)}
        expected = '{"key": "1970-01-01T00:00:00"}'
        result = json.dumps(data, cls=DateTimeEncoder)
        assert result == expected

    def test_super_class_raise_type_error(self, datetime_encoder):
        with pytest.raises(TypeError):
            datetime_encoder.default(0)


class TestObjectEncoder:
    def test_encode_datetime(self):
        data = {"key": datetime.utcfromtimestamp(0), "key2": "test_string"}
        expected = '{"key": {"__custom__": true, "__module__": "datetime", "__name__": "datetime"}, "key2": "test_string"}'  # noqa
        result = json.dumps(data, cls=ObjectEncoder)
        assert result == expected

    def test_encode_with_dict_attribute(self):
        class ObjectForEncoding:
            key = "value"

            def __dict__(self):
                return {"key": self.key}

        data = {"test_obj": ObjectForEncoding()}
        expected = '{"test_obj": "ObjectForEncoding"}'
        assert json.dumps(data, cls=ObjectEncoder) == expected

    def test_encode_with_to_json(self):
        class ObjectForEncoding:
            key = "value"

            def to_json(self):
                return json.dumps({"key": self.key})

        data = {"test_obj": ObjectForEncoding()}
        expected = '{"test_obj": {"__custom__": true, "__module__": "builtins", "__name__": "str"}}'  # noqa
        assert json.dumps(data, cls=ObjectEncoder) == expected

    def test_encode_timedelta(self):
        data = {"test_obj": timedelta(hours=1)}
        expected = '{"test_obj": "1:00:00"}'
        assert json.dumps(data, cls=ObjectEncoder) == expected
