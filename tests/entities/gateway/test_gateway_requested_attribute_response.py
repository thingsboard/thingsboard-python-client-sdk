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
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse


def make_entry(key, value):
    return AttributeEntry(key=key, value=value)


def test_post_init_and_event_type_enforcement():
    resp = GatewayRequestedAttributeResponse(device_name="dev1", request_id=1)
    # Even if you pass no event_type, __post_init__ sets it
    assert resp.event_type == GatewayEventType.DEVICE_REQUESTED_ATTRIBUTE_RESPONSE


def test_repr_contains_expected_fields():
    shared = [make_entry("s1", "v1")]
    client = [make_entry("c1", "v2")]
    resp = GatewayRequestedAttributeResponse(
        device_name="devX",
        request_id=42,
        shared=shared,
        client=client
    )
    r = repr(resp)
    assert "GatewayRequestedAttributeResponse" in r
    assert "devX" in r
    assert "42" in r
    assert "s1" in r
    assert "c1" in r


def test_getitem_finds_in_shared_first():
    shared = [make_entry("foo", "bar")]
    client = [make_entry("foo", "baz")]  # same key, should not be used if found in shared
    resp = GatewayRequestedAttributeResponse(shared=shared, client=client)
    assert resp["foo"] == "bar"  # found in shared first


def test_getitem_finds_in_client_if_not_in_shared():
    shared = [make_entry("other", 123)]
    client = [make_entry("foo", "baz")]
    resp = GatewayRequestedAttributeResponse(shared=shared, client=client)
    assert resp["foo"] == "baz"


def test_getitem_raises_keyerror_if_not_found():
    resp = GatewayRequestedAttributeResponse(
        shared=[make_entry("one", 1)],
        client=[make_entry("two", 2)]
    )
    with pytest.raises(KeyError) as e:
        _ = resp["missing"]
    assert "Key 'missing'" in str(e.value)


def test_shared_keys_and_client_keys():
    shared = [make_entry("s1", "v1"), make_entry("s2", "v2")]
    client = [make_entry("c1", "v3")]
    resp = GatewayRequestedAttributeResponse(shared=shared, client=client)
    assert resp.shared_keys() == ["s1", "s2"]
    assert resp.client_keys() == ["c1"]


def test_get_shared_and_client_success():
    shared = [make_entry("sh", "sv")]
    client = [make_entry("cl", "cv")]
    resp = GatewayRequestedAttributeResponse(shared=shared, client=client)
    assert resp.get_shared("sh") == "sv"
    assert resp.get_client("cl") == "cv"


def test_get_shared_and_client_with_default_on_missing():
    shared = [make_entry("sh", "sv")]
    client = [make_entry("cl", "cv")]
    resp = GatewayRequestedAttributeResponse(shared=shared, client=client)
    assert resp.get_shared("missing", default="def") == "def"
    assert resp.get_client("missing", default="def") == "def"


def test_get_shared_and_client_when_none():
    resp = GatewayRequestedAttributeResponse(shared=None, client=None)
    assert resp.get_shared("any", default="d") == "d"
    assert resp.get_client("any", default="d") == "d"


def test_as_dict_returns_expected_dict(monkeypatch):
    shared_entry = make_entry("s1", "v1")
    client_entry = make_entry("c1", "v2")
    called = {}

    def fake_as_dict_self():
        called.setdefault("calls", []).append(1)
        return {"key": "dummy", "value": "dummy"}

    monkeypatch.setattr(shared_entry, "as_dict", fake_as_dict_self)
    monkeypatch.setattr(client_entry, "as_dict", fake_as_dict_self)

    resp = GatewayRequestedAttributeResponse(
        shared=[shared_entry],
        client=[client_entry]
    )
    d = resp.as_dict()
    assert "shared" in d
    assert "client" in d
    assert isinstance(d["shared"], list)
    assert isinstance(d["client"], list)
    # Ensure our fake_as_dict was called twice
    assert len(called["calls"]) == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
