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

from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest


def test_direct_instantiation_not_allowed():
    """
    Ensures dataclass can't be instantiated directly and guides to deserializer.
    """
    with pytest.raises(TypeError) as exc:
        GatewayRPCRequest()  # type: ignore[call-arg]
    assert "Direct instantiation" in str(exc.value)


def test_deserialize_with_int_id_and_params():
    """
    Valid deserialization: numeric request_id and explicit params.
    """
    payload = {
        "device": "pump-1",
        "data": {
            "id": 42,
            "method": "reboot",
            "params": {"delay": 5}
        }
    }
    req = GatewayRPCRequest._deserialize_from_dict(payload)

    assert isinstance(req, GatewayRPCRequest)
    assert req.request_id == 42
    assert req.device_name == "pump-1"
    assert req.method == "reboot"
    assert req.params == {"delay": 5}
    assert req.event_type == GatewayEventType.DEVICE_RPC_REQUEST

    # __repr__ and __str__ should be identical and informative
    expected_repr = "RPCRequest(id=42, device_name=pump-1, method=reboot, params={'delay': 5})"
    assert repr(req) == expected_repr
    assert str(req) == expected_repr


def test_deserialize_with_str_id_and_no_params():
    """
    Valid deserialization: string request_id and missing params (defaults to None).
    """
    payload = {
        "device": "sensor-A",
        "data": {
            "id": "rpc-001",
            "method": "ping"
            # no "params"
        }
    }
    req = GatewayRPCRequest._deserialize_from_dict(payload)

    assert req.request_id == "rpc-001"
    assert req.device_name == "sensor-A"
    assert req.method == "ping"
    assert req.params is None
    assert req.event_type == GatewayEventType.DEVICE_RPC_REQUEST

    expected_repr = "RPCRequest(id=rpc-001, device_name=sensor-A, method=ping, params=None)"
    assert repr(req) == expected_repr
    assert str(req) == expected_repr


def test_missing_device_raises_value_error():
    """
    Missing top-level 'device' should raise a clear ValueError.
    """
    payload = {
        # "device": "x",
        "data": {"id": 1, "method": "ping"}
    }
    with pytest.raises(ValueError) as exc:
        GatewayRPCRequest._deserialize_from_dict(payload)
    assert "Missing device name" in str(exc.value)


def test_missing_data_key_raises_key_error():
    """
    The implementation accesses data['data'] directly; if absent, a KeyError is expected.
    """
    payload = {
        "device": "dev-x"
        # no "data"
    }
    with pytest.raises(KeyError) as exc:
        GatewayRPCRequest._deserialize_from_dict(payload)
    assert "'data'" in str(exc.value)  # KeyError message includes missing key


@pytest.mark.parametrize("bad_id", [None, 3.14, {"n": 1}, ["1"]])
def test_invalid_request_id_type_raises_value_error(bad_id):
    """
    request_id must be int or str; anything else should raise ValueError.
    """
    payload = {
        "device": "dev-y",
        "data": {"id": bad_id, "method": "ping"}
    }
    with pytest.raises(ValueError) as exc:
        GatewayRPCRequest._deserialize_from_dict(payload)
    # The current implementation uses a generic message; assert on the key phrase.
    assert "request id" in str(exc.value).lower()


def test_missing_method_raises_value_error():
    """
    Missing 'method' inside 'data' should raise ValueError.
    """
    payload = {
        "device": "dev-z",
        "data": {"id": 7}  # no "method"
    }
    with pytest.raises(ValueError) as exc:
        GatewayRPCRequest._deserialize_from_dict(payload)
    assert "Missing 'method'" in str(exc.value)


def test_event_type_is_device_rpc_request():
    """
    Sanity check to ensure event_type is set correctly by the deserializer.
    """
    payload = {
        "device": "gw-1",
        "data": {
            "id": 101,
            "method": "set_threshold",
            "params": {"value": 12.3}
        }
    }
    req = GatewayRPCRequest._deserialize_from_dict(payload)
    assert req.event_type == GatewayEventType.DEVICE_RPC_REQUEST


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
