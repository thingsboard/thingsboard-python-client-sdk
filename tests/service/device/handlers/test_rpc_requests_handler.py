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

from typing import Union, List

import pytest

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest
from tb_mqtt_client.entities.data.provisioning_response import ProvisioningResponse
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse, RPCStatus
from tb_mqtt_client.service.device.handlers.rpc_requests_handler import RPCRequestsHandler
from tb_mqtt_client.service.device.message_adapter import MessageAdapter


class DummyMessageAdapter(MessageAdapter):
    def build_uplink_messages(self, messages: List[MqttPublishMessage]) -> List[MqttPublishMessage]:
        pass

    def build_attribute_request(self, request: AttributeRequest) -> MqttPublishMessage:
        pass

    def build_claim_request(self, claim_request) -> MqttPublishMessage:
        pass

    def build_rpc_request(self, rpc_request: RPCRequest) -> MqttPublishMessage:
        pass

    def build_rpc_response(self, rpc_response: RPCResponse) -> MqttPublishMessage:
        pass

    def build_provision_request(self, provision_request) -> MqttPublishMessage:
        pass

    def parse_requested_attribute_response(self, topic: str, payload: bytes) -> RequestedAttributeResponse:
        pass

    def parse_attribute_update(self, payload: bytes) -> AttributeUpdate:
        pass

    def parse_rpc_response(self, topic: str, payload: Union[bytes, Exception]) -> RPCResponse:
        pass

    def parse_provisioning_response(self, provisioning_request: ProvisioningRequest,
                                    payload: bytes) -> ProvisioningResponse:
        pass

    def parse_rpc_request(self, topic, payload):
        # Simulate payload as dict with required keys
        data = {"method": "testMethod", "params": {"foo": "bar"}}
        return RPCRequest._deserialize_from_dict(42, data)


@pytest.mark.asyncio
async def test_handle_no_callback_returns_none():
    handler = RPCRequestsHandler()
    handler.set_message_adapter(DummyMessageAdapter())
    # No callback set
    result = await handler.handle("topic", b"payload")
    assert result is None


@pytest.mark.asyncio
async def test_handle_no_message_adapter_returns_none():
    handler = RPCRequestsHandler()

    async def cb(_):
        return RPCResponse.build(1, result={"ok": True})

    handler.set_callback(cb)
    # No message adapter set
    result = await handler.handle("topic", b"payload")
    assert result is None


@pytest.mark.asyncio
async def test_handle_callback_returns_valid_response():
    handler = RPCRequestsHandler()
    handler.set_message_adapter(DummyMessageAdapter())

    async def cb(rpc_request: RPCRequest):
        assert isinstance(rpc_request, RPCRequest)
        return RPCResponse.build(rpc_request.request_id, result={"success": True})

    handler.set_callback(cb)
    result = await handler.handle("topic", b"payload")
    assert isinstance(result, RPCResponse)
    assert result.status == RPCStatus.SUCCESS
    assert result.result == {"success": True}
    assert result.error is None


@pytest.mark.asyncio
async def test_handle_callback_returns_invalid_type():
    handler = RPCRequestsHandler()
    handler.set_message_adapter(DummyMessageAdapter())

    async def cb(_):
        return "not-an-rpc-response"

    handler.set_callback(cb)
    result = await handler.handle("topic", b"payload")
    assert result is None


@pytest.mark.asyncio
async def test_handle_parse_raises_exception():
    class BadAdapter(DummyMessageAdapter):
        def parse_rpc_request(self, topic, payload):
            raise RuntimeError("bad parse")

    handler = RPCRequestsHandler()
    handler.set_message_adapter(BadAdapter())

    async def cb(_):
        return RPCResponse.build(1, result={})

    handler.set_callback(cb)
    result = await handler.handle("topic", b"payload")
    assert result is None


def test_set_message_adapter_invalid_type():
    handler = RPCRequestsHandler()
    with pytest.raises(ValueError):
        handler.set_message_adapter("not-a-message-adapter")


def test_rpc_response_build_success_and_repr_and_payload():
    resp = RPCResponse.build(1, result={"x": 1})
    assert resp.status == RPCStatus.SUCCESS
    assert resp.result == {"x": 1}
    assert resp.error is None
    assert "RPCResponse" in repr(resp)
    assert resp.to_payload_format() == {"result": {"x": 1}}


def test_rpc_response_build_with_str_error():
    resp = RPCResponse.build(1, error="something went wrong")
    assert resp.status == RPCStatus.ERROR
    assert resp.error == "something went wrong"
    assert "error" in resp.to_payload_format()


def test_rpc_response_build_with_dict_error():
    err_dict = {"code": 123}
    resp = RPCResponse.build(1, error=err_dict)
    assert resp.status == RPCStatus.ERROR
    assert resp.error == err_dict


def test_rpc_response_build_with_exception_error():
    exc = ValueError("bad value")
    resp = RPCResponse.build(1, error=exc)
    assert resp.status == RPCStatus.ERROR
    assert isinstance(resp.error, dict)
    assert "message" in resp.error
    assert "type" in resp.error
    assert "details" in resp.error


def test_rpc_response_invalid_error_type():
    with pytest.raises(ValueError):
        RPCResponse.build(1, error=object())


def test_rpc_response_direct_init_disallowed():
    with pytest.raises(TypeError):
        RPCResponse(1, result={})


@pytest.mark.asyncio
async def test_rpc_request_build_and_str_and_payload_format():
    req = await RPCRequest.build("myMethod", params={"y": 2})
    assert req.method == "myMethod"
    assert req.params == {"y": 2}
    assert "myMethod" in str(req)
    payload = req.to_payload_format()
    assert payload["method"] == "myMethod"
    assert payload["params"] == {"y": 2}

@pytest.mark.asyncio
async def test_rpc_request_build_invalid_method_type():
    with pytest.raises(ValueError):
        await RPCRequest.build(123)


def test_rpc_request_deserialize_missing_method():
    with pytest.raises(ValueError):
        RPCRequest._deserialize_from_dict(1, {})


def test_rpc_request_deserialize_invalid_id_type():
    with pytest.raises(ValueError):
        RPCRequest._deserialize_from_dict(object(), {"method": "x"})


def test_rpc_request_direct_init_disallowed():
    with pytest.raises(TypeError):
        RPCRequest(1, "method")


def test_rpc_request_to_payload_format_without_params():
    req = RPCRequest._deserialize_from_dict(5, {"method": "noParams"})
    payload = req.to_payload_format()
    assert "params" not in payload


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
