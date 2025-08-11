#  Copyright 2025 ThingsBoard
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import pytest

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse


@pytest.fixture
def shared_attrs():
    return [AttributeEntry("shared_key_1", "shared_value_1"), AttributeEntry("shared_key_2", 42)]


@pytest.fixture
def client_attrs():
    return [AttributeEntry("client_key_1", "client_value_1"), AttributeEntry("client_key_2", True)]


@pytest.fixture
def response(shared_attrs, client_attrs):
    return RequestedAttributeResponse(request_id=1001, shared=shared_attrs, client=client_attrs)


def test_repr(response):
    output = repr(response)
    assert "RequestedAttributeResponse" in output
    assert "request_id=1001" in output
    assert "shared_key_1" in output
    assert "client_key_1" in output


def test_getitem_shared_key(response):
    assert response["shared_key_1"] == "shared_value_1"
    assert response["shared_key_2"] == 42


def test_getitem_client_key(response):
    assert response["client_key_1"] == "client_value_1"
    assert response["client_key_2"] is True


def test_getitem_key_error(response):
    with pytest.raises(KeyError, match="Key 'nonexistent' not found"):
        _ = response["nonexistent"]


def test_get_shared_success(shared_attrs, client_attrs):
    response = RequestedAttributeResponse(5, shared_attrs, client_attrs)
    assert response.get_shared("shared_key_1") == "shared_value_1"
    assert response.get_shared("shared_key_2") == 42


def test_get_shared_default(response):
    assert response.get_shared("unknown", default="fallback") == "fallback"
    assert response.get_shared("unknown") is None


def test_get_client_success(response):
    assert response.get_client("client_key_1") == "client_value_1"
    assert response.get_client("client_key_2") is True


def test_get_client_default(response):
    assert response.get_client("unknown", default=0) == 0
    assert response.get_client("unknown") is None


def test_shared_keys(response):
    keys = response.shared_keys()
    assert keys == ["shared_key_1", "shared_key_2"]


def test_client_keys(response):
    keys = response.client_keys()
    assert keys == ["client_key_1", "client_key_2"]


def test_as_dict(response):
    result = response.as_dict()
    assert isinstance(result, dict)
    assert "shared" in result
    assert "client" in result
    assert result["shared"][0]["key"] == "shared_key_1"
    assert result["client"][1]["value"] is True


def test_from_dict_full():
    data = {
        "request_id": 77,
        "shared": {"temp": 25, "mode": "cool"},
        "client": {"state": "on", "power": 100}
    }
    resp = RequestedAttributeResponse.from_dict(data)
    assert isinstance(resp, RequestedAttributeResponse)
    assert resp.request_id == 77
    assert resp.get_shared("temp") == 25
    assert resp.get_client("power") == 100


def test_from_dict_missing_request_id():
    data = {
        "shared": {"x": 1},
        "client": {"y": 2}
    }
    resp = RequestedAttributeResponse.from_dict(data)
    assert resp.request_id == -1
    assert resp.get_shared("x") == 1
    assert resp.get_client("y") == 2


def test_from_dict_empty():
    resp = RequestedAttributeResponse.from_dict({})
    assert resp.request_id == -1
    assert resp.shared == []
    assert resp.client == []
