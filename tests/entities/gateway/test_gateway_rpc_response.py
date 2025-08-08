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

import re
import pytest

from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.data.rpc_response import RPCStatus
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse


def test_direct_instantiation_not_allowed():
    with pytest.raises(TypeError) as exc:
        GatewayRPCResponse()  # type: ignore[call-arg]
    assert "Direct instantiation of GatewayRPCResponse is not allowed" in str(exc.value)


def test_build_success_with_result_only():
    req = GatewayRPCResponse.build(
        device_name="pump-1",
        request_id=101,
        result={"ok": True}
    )
    assert isinstance(req, GatewayRPCResponse)
    assert req.device_name == "pump-1"
    assert req.request_id == 101
    assert req.status == RPCStatus.SUCCESS
    assert req.error is None
    assert req.result == {"ok": True}
    assert req.event_type == GatewayEventType.DEVICE_RPC_RESPONSE

    expected_repr = (
        "GatewayRPCResponse(device_name=pump-1, request_id=101, result={'ok': True}, error=None)"
    )
    assert repr(req) == expected_repr

    payload = req.to_payload_format()
    assert payload == {"device": "pump-1", "id": 101, "data": {"result": {"ok": True}}}


def test_build_with_error_string():
    req = GatewayRPCResponse.build(
        device_name="sensor-A",
        request_id=7,
        error="boom"
    )
    assert req.status == RPCStatus.ERROR
    assert req.error == "boom"
    assert req.result is None

    payload = req.to_payload_format()
    assert payload == {"device": "sensor-A", "id": 7, "data": {"error": "boom"}}


def test_build_with_error_dict():
    err = {"message": "bad-things", "code": 500}
    req = GatewayRPCResponse.build(
        device_name="sensor-B",
        request_id=8,
        error=err
    )
    assert req.status == RPCStatus.ERROR
    assert req.error == err
    assert req.result is None

    payload = req.to_payload_format()
    assert payload == {"device": "sensor-B", "id": 8, "data": {"error": err}}


def test_build_with_exception_converted_to_struct():
    class CustomError(RuntimeError):
        pass

    req = GatewayRPCResponse.build(
        device_name="dev-ex",
        request_id=999,
        error=CustomError("kaput")
    )

    assert req.status == RPCStatus.ERROR
    assert isinstance(req.error, dict)
    assert req.error.get("message") == "kaput"
    assert req.error.get("type") == "CustomError"
    assert isinstance(req.error.get("details"), str)
    assert "CustomError" in req.error["details"]

    payload = req.to_payload_format()
    assert payload["device"] == "dev-ex"
    assert payload["id"] == 999
    assert "error" in payload["data"]
    assert payload["data"]["error"]["type"] == "CustomError"


def test_build_with_result_and_error_includes_both_in_payload():
    req = GatewayRPCResponse.build(
        device_name="dev-mix",
        request_id=321,
        result={"partial": True},
        error="some error"
    )
    assert req.status == RPCStatus.ERROR
    assert req.result == {"partial": True}
    assert req.error == "some error"

    payload = req.to_payload_format()
    assert payload == {
        "device": "dev-mix",
        "id": 321,
        "data": {"result": {"partial": True}, "error": "some error"},
    }


@pytest.mark.parametrize("bad_device", ["", 123, None, object()])
def test_build_invalid_device_name_raises_value_error(bad_device):
    with pytest.raises(ValueError) as exc:
        GatewayRPCResponse.build(device_name=bad_device, request_id=1)  # type: ignore[arg-type]
    assert "Device name must be a non-empty string" in str(exc.value)


@pytest.mark.parametrize("bad_error", [123, 3.14, ["oops"], set(), object()])
def test_build_invalid_error_type_raises_value_error(bad_error):
    with pytest.raises(ValueError) as exc:
        GatewayRPCResponse.build(device_name="dev-x", request_id=1, error=bad_error)  # type: ignore[arg-type]
    assert "Error must be a string, dictionary, or an exception instance" in str(exc.value)


def test_build_invalid_result_fails_json_validation():
    class NotJson:
        pass

    with pytest.raises(ValueError) as exc:
        GatewayRPCResponse.build(device_name="dev-j", request_id=2, result=NotJson())
    assert re.search(r"(json|compatible|type)", str(exc.value), re.IGNORECASE)


def test_build_invalid_error_dict_fails_json_validation():
    class NotJson:
        pass

    with pytest.raises(ValueError) as exc:
        GatewayRPCResponse.build(
            device_name="dev-je",
            request_id=3,
            error={"bad": NotJson()}
        )
    assert re.search(r"(json|compatible|type)", str(exc.value), re.IGNORECASE)


def test_event_type_is_device_rpc_response():
    req = GatewayRPCResponse.build(device_name="gw-1", request_id=55, result=True)
    assert req.event_type == GatewayEventType.DEVICE_RPC_RESPONSE


def test_repr_includes_all_fields():
    req = GatewayRPCResponse.build(device_name="repr-dev", request_id=42, result={"x": 1})
    assert repr(req) == "GatewayRPCResponse(device_name=repr-dev, request_id=42, result={'x': 1}, error=None)"


def test_immutability_enforced_by_frozen_dataclass():
    req = GatewayRPCResponse.build(device_name="immu", request_id=1, result=None)
    with pytest.raises((AttributeError, TypeError)):
        setattr(req, "status", RPCStatus.ERROR)


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
