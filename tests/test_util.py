import pytest  # noqa

from util import hf_size, apply_transformation


class TestUtil:
    def test_hf_size_zero_bytes(self):
        assert hf_size(0) == "0B"

    def test_hf_size_string_arg(self):
        assert hf_size("123") == "123.0 B"

    def test_hf_format_kb(self):
        assert hf_size(123456) == "120.56 KB"

    def test_hf_format_mb(self):
        assert hf_size(1200000) == "1.14 MB"

    def test_hf_format_gb(self):
        assert hf_size(1200000000) == "1.12 GB"

    def test_apply_transformation_nested_dict(self):
        data = {
            "key": "value",
            "dict_key": {
                "nested_key": "10",
                "nested_dict_key": {"nested_key": "nested_value"},
            },
        }
        expected = {
            "key": "VALUE",
            "dict_key": {
                "nested_key": "10",
                "nested_dict_key": {"nested_key": "NESTED_VALUE"},
            },
        }
        result = apply_transformation(data, lambda x: str(x).upper())
        assert str(result) == str(expected)
