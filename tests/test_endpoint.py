import pytest  # noqa

import functools

from collector import Endpoint
from flask_sqlalchemy import Model


@pytest.fixture
def prototype(conf):
    yield functools.partial(Endpoint, name="ep_test", model="api.models.FracSchedule")


class TestEndpoint:
    def test_endpoint_loaded(self, endpoint):
        endpoint

    def test_load_from_config(self, conf):
        ep = Endpoint.load_from_config(conf).get(conf.FRAC_SCHEDULE_TABLE_NAME)
        assert ep.name == conf.FRAC_SCHEDULE_TABLE_NAME

    def test_model_found(self, endpoint):
        assert Model in endpoint.model.__mro__

    def test_exclustions_are_valid(self, prototype):
        expected = ["column1", "column2", "column3"]
        ep = prototype(exclude=expected)
        assert ep.exclude == expected

    def test_create_from_dict(self):
        Endpoint.from_dict("ep_from_dict", {"model": "api.models.FracSchedule"})

    def test_alias_mapping_properties_are_valid(self, prototype):
        mappings = {
            "aliases": {
                "Original Column Name": "aliased_column_name",
                "OtherColumn": "other_column",
            }
        }
        ep = prototype(mappings=mappings)
        assert ep.alias_map == mappings["aliases"]
        assert ep.mapped_aliases == list(mappings["aliases"].values())
        assert ep.mapped_names == list(mappings["aliases"].keys())

    def test_known_columns_are_valid(self, prototype):
        mappings = {
            "aliases": {
                "Original Column Name": "aliased_column_name",
                "OtherColumn": "other_column",
            },
        }
        exclusions = ["ArbitraryColumn", "Irrelevant Column"]
        ep = prototype(mappings=mappings, exclude=exclusions)
        expected = exclusions + list(mappings["aliases"].keys())
        assert ep.known_columns == expected
