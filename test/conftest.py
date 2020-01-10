import json
import os
import re

import pytest
from requests_mock import ANY

import collector.endpoint
from collector.collector import Collector
from collector.endpoint import Endpoint
from collector.request import Request
from collector.requestor import Requestor
from collector.token_manager import TokenManager
from config import TestingConfig


# conf = TestingConfig()
# endpoints = collector.endpoint.load_from_config(conf)
# functions = config.functions


@pytest.fixture
def conf():
    yield TestingConfig()


@pytest.fixture
def endpoints(conf):
    yield collector.endpoint.load_from_config(conf)


@pytest.fixture
def functions(conf):
    yield conf.functions


@pytest.fixture
def endpoint(endpoints):
    yield endpoints.get("complex")


@pytest.fixture
def endpoint_simple(endpoints):
    yield endpoints.get("simple")


@pytest.fixture
def token_manager_legacy(conf):  # TODO: needs to be mocked
    yield TokenManager(**conf.api_params)


@pytest.fixture
def requestor(conf, endpoint, functions):
    yield Requestor(conf.API_BASE_URL, endpoint, functions)


@pytest.fixture
def requestor_simple(conf, endpoint_simple, functions):
    yield Requestor(conf.API_BASE_URL, endpoint_simple, functions)


@pytest.fixture(autouse=True)
def mocker(conf, requests_mock):

    requests_mock.register_uri(
        "POST",
        f"{conf.API_BASE_URL}{conf.API_TOKEN_PATH}",
        json={
            "access_token": "testtoken",
            "expires_at": 1577149458,
            "expires_in": 604800,
            "token_type": "Bearer",
        },
    )

    requests_mock.register_uri(
        "GET", re.compile(f"{conf.API_BASE_URL}/v3/path/.*/subpath/.*"), json={}
    )
    requests_mock.register_uri("GET", ANY, json={})
    # yield m


@pytest.fixture
def req(conf, mocker):  # pylint: disable=unused-argument
    tm = TokenManager.from_app_config(conf)
    yield Request(
        "GET",
        f"{conf.API_BASE_URL}/path/1/subpath/2/values",
        headers={"Authorization": tm.get_token()},
    )


@pytest.fixture
def nested_json(conf):
    path = os.path.join(conf.CONFIG_BASEPATH, "nested_data.json")
    with open(path, "r") as f:
        return json.load(f)


@pytest.fixture
def normalized_json(conf):
    path = os.path.join(conf.CONFIG_BASEPATH, "normalized.json")
    with open(path, "r") as f:
        return json.load(f)
