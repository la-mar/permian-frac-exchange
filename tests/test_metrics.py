import pytest  # noqa

from metrics import load, post, post_event, post_heartbeat, to_tags


class TestMetrics:
    def test_load_datadog_lib(self, conf):
        conf.DATADOG_API_KEY = "pretend_api_key"
        conf.DATADOG_APP_KEY = "pretend_app_key"
        conf.DATADOG_Enabled = True

        load(conf)

    def test_dict_to_tags(self):
        data = {
            "tag_name": "tag_value",
            "tag_name2": "tag_value2",
        }
        assert to_tags(data) == ["tag_name:tag_value", "tag_name2:tag_value2"]

    def test_list_to_tags(self):
        data = ["tag_name:tag_value", "tag_name2:tag_value2"]
        assert to_tags(data) == data

    def test_comma_delimited_string_to_tags(self):
        data = "tag_name:tag_value,tag_name2:tag_value2"
        assert to_tags(data) == ["tag_name:tag_value", "tag_name2:tag_value2"]
