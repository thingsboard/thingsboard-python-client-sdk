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

from unittest.mock import patch

import pytest

from tb_mqtt_client.common.request_id_generator import RPCRequestIdProducer
from tb_mqtt_client.entities.data.rpc_request import RPCRequest


@pytest.mark.asyncio
async def test_rpc_request_build_valid():
    method = "reboot"
    params = {"delay": 5}
    with patch.object(RPCRequestIdProducer, "get_next", return_value=42):
        request = await RPCRequest.build(method, params)

    assert request.request_id == 42
    assert request.method == method
    assert request.params == params
    assert request.to_payload_format() == {
        "id": 42,
        "method": "reboot",
        "params": {"delay": 5}
    }


def test_rpc_request_direct_instantiation_raises():
    with pytest.raises(TypeError, match="Direct instantiation of RPCRequest is not allowed"):
        RPCRequest(1, "reboot")


@pytest.mark.asyncio
async def test_rpc_request_build_with_none_params():
    with patch.object(RPCRequestIdProducer, "get_next", return_value="abc123"):
        request = await RPCRequest.build("ping")

    assert request.params is None
    assert request.request_id == "abc123"
    assert request.to_payload_format() == {
        "id": "abc123",
        "method": "ping"
    }


@pytest.mark.asyncio
async def test_rpc_request_build_invalid_method_type():
    with pytest.raises(ValueError, match="Method must be a string"):
        await RPCRequest.build(123, {"key": "value"})


def test_rpc_request_deserialize_valid():
    data = {"method": "getStatus", "params": {"verbose": True}}
    req = RPCRequest._deserialize_from_dict(1001, data)

    assert req.request_id == 1001
    assert req.method == "getStatus"
    assert req.params == {"verbose": True}
    assert req.to_payload_format() == {
        "id": 1001,
        "method": "getStatus",
        "params": {"verbose": True}
    }


def test_rpc_request_deserialize_missing_id():
    with pytest.raises(ValueError, match="Missing request id"):
        RPCRequest._deserialize_from_dict(None, {"method": "test"})


def test_rpc_request_deserialize_missing_method():
    with pytest.raises(ValueError, match="Missing 'method' in RPC request"):
        RPCRequest._deserialize_from_dict(1, {})


def test_rpc_request_repr():
    data = {"method": "info", "params": {"a": 1}}
    req = RPCRequest._deserialize_from_dict(42, data)
    assert repr(req) == "RPCRequest(id=42, method=info, params={'a': 1})"
