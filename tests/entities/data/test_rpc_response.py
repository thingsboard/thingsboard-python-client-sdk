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

from tb_mqtt_client.entities.data.rpc_response import RPCResponse, RPCStatus


class NonSerializable:
    pass


def test_rpc_status_str():
    assert str(RPCStatus.SUCCESS) == "SUCCESS"
    assert str(RPCStatus.ERROR) == "ERROR"
    assert str(RPCStatus.TIMEOUT) == "TIMEOUT"
    assert str(RPCStatus.NOT_FOUND) == "NOT_FOUND"


def test_rpc_response_direct_instantiation_raises():
    with pytest.raises(TypeError):
        RPCResponse(1, result="test")


def test_rpc_response_success():
    response = RPCResponse.build(request_id=1, result={"key": "value"})
    assert response.request_id == 1
    assert response.status == RPCStatus.SUCCESS
    assert response.result == {"key": "value"}
    assert response.error is None
    assert response.to_payload_format() == {"result": {"key": "value"}}


def test_rpc_response_error_with_string():
    response = RPCResponse.build(request_id="req-1", error="Something went wrong")
    assert response.request_id == "req-1"
    assert response.status == RPCStatus.ERROR
    assert response.error == "Something went wrong"
    assert response.result is None
    assert response.to_payload_format() == {"error": "Something went wrong"}


def test_rpc_response_error_with_dict():
    error_dict = {"code": 500, "message": "Internal Error"}
    response = RPCResponse.build(request_id="req-2", error=error_dict)
    assert response.request_id == "req-2"
    assert response.status == RPCStatus.ERROR
    assert response.error == error_dict
    assert response.result is None
    assert response.to_payload_format() == {"error": error_dict}


def test_rpc_response_error_with_exception():
    try:
        raise ValueError("Something failed")
    except ValueError as ex:
        response = RPCResponse.build(request_id=42, error=ex)

    assert response.status == RPCStatus.ERROR
    assert isinstance(response.error, dict)
    assert "message" in response.error
    assert "type" in response.error
    assert "details" in response.error
    assert response.error["type"] == "ValueError"
    assert response.error["message"] == "Something failed"
    assert "raise ValueError(\"Something failed\")" in response.error["details"]


def test_rpc_response_invalid_error_type():
    with pytest.raises(ValueError):
        RPCResponse.build(request_id=1, error=NonSerializable())


def test_rpc_response_invalid_result_type():
    with pytest.raises(ValueError):
        RPCResponse.build(request_id=1, result=NonSerializable())
