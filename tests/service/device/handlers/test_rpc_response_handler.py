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

import asyncio
from typing import List

import pytest

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest
from tb_mqtt_client.entities.data.provisioning_response import ProvisioningResponse
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.device.handlers.rpc_response_handler import RPCResponseHandler
from tb_mqtt_client.service.device.message_adapter import MessageAdapter, JsonMessageAdapter


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

    def parse_rpc_request(self, topic: str, payload: bytes) -> RPCRequest:
        pass

    def parse_provisioning_response(self, provisioning_request: ProvisioningRequest,
                                    payload: bytes) -> ProvisioningResponse:
        pass

    def __init__(self, rpc_response):
        super().__init__()
        self._resp = rpc_response

    def parse_rpc_response(self, topic, payload):
        return self._resp


@pytest.mark.asyncio
async def test_set_message_adapter_invalid_type():
    handler = RPCResponseHandler()
    with pytest.raises(ValueError):
        handler.set_message_adapter("not-an-adapter")


@pytest.mark.asyncio
async def test_register_and_handle_success_with_callback():
    handler = RPCResponseHandler()
    resp = RPCResponse.build(1, result={"ok": True})
    handler.set_message_adapter(DummyMessageAdapter(resp))

    called = {}

    async def cb(r: RPCResponse):
        called["cb"] = r

    fut = handler.register_request(1, callback=cb)
    await handler.handle("topic", b"payload")
    assert fut.done()
    result = fut.result()
    assert isinstance(result, RPCResponse)
    assert result.result == {"ok": True}
    assert called["cb"] == resp


@pytest.mark.asyncio
async def test_register_request_duplicate_id_raises():
    handler = RPCResponseHandler()
    handler._pending_rpc_requests[5] = (asyncio.get_event_loop().create_future(), None)
    with pytest.raises(RuntimeError):
        await handler.register_request(5)


@pytest.mark.asyncio
async def test_handle_no_message_adapter_uses_json_adapter():
    handler = RPCResponseHandler()
    # Build a real RPCResponse and serialize to payload
    resp = RPCResponse.build(99, result={"foo": "bar"})
    message = JsonMessageAdapter().build_rpc_response(resp)
    # Register request so we can match
    fut = handler.register_request(99)
    # Should succeed even without explicitly setting message adapter
    await handler.handle("v1/devices/me/rpc/response/99", message.payload)
    assert fut.done()
    assert fut.result().result == {'result': {"foo": "bar"}}


@pytest.mark.asyncio
async def test_handle_no_message_adapter_uses_json_adapter_with_error_rpc():
    handler = RPCResponseHandler()
    # Build a real RPCResponse and serialize to payload
    resp = RPCResponse.build(99, error="Some error occurred")
    message = JsonMessageAdapter().build_rpc_response(resp)
    # Register request so we can match
    fut = handler.register_request(99)
    # Should succeed even without explicitly setting message adapter
    await handler.handle("v1/devices/me/rpc/response/99", message.payload)
    assert fut.done()
    assert fut.result().result == {'error': "Some error occurred"}


@pytest.mark.asyncio
async def test_handle_response_for_unknown_request_id():
    handler = RPCResponseHandler()
    resp = RPCResponse.build("abc", result={"zzz": 1})
    # No registered request with "abc"
    handler.set_message_adapter(DummyMessageAdapter(resp))
    await handler.handle("topic", b"payload")  # Should log warning and return
    # Nothing to assert except no crash


@pytest.mark.asyncio
async def test_handle_with_no_future():
    handler = RPCResponseHandler()
    resp = RPCResponse.build(777, result={"x": 1})
    handler.set_message_adapter(DummyMessageAdapter(resp))
    handler._pending_rpc_requests[777] = (None, None)
    await handler.handle("topic", b"payload")  # Should log warning and return


@pytest.mark.asyncio
async def test_handle_callback_exception_sets_future_exception():
    handler = RPCResponseHandler()
    resp = RPCResponse.build(42, result={"ok": True})
    handler.set_message_adapter(DummyMessageAdapter(resp))

    async def bad_cb(_):
        raise RuntimeError("bad")

    fut = handler.register_request(42, callback=bad_cb)
    await handler.handle("topic", b"payload")
    assert fut.done()
    with pytest.raises(RuntimeError):
        fut.result()


@pytest.mark.asyncio
async def test_handle_with_error_field_sets_future_exception():
    handler = RPCResponseHandler()
    resp = RPCResponse.build(321, error="fail")
    handler.set_message_adapter(DummyMessageAdapter(resp))
    fut = handler.register_request(321)
    await handler.handle("topic", b"payload")
    assert fut.done()
    with pytest.raises(Exception) as e:
        fut.result()
    assert "fail" in str(e.value)


@pytest.mark.asyncio
async def test_clear_cancels_pending_futures():
    handler = RPCResponseHandler()
    f1 = asyncio.get_event_loop().create_future()
    handler._pending_rpc_requests[1] = (f1, None)
    handler.clear()
    assert f1.cancelled()
    assert handler._pending_rpc_requests == {}


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
