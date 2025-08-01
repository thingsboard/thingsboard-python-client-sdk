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
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest
from tb_mqtt_client.entities.data.provisioning_response import ProvisioningResponse
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.device.handlers.requested_attributes_response_handler import \
    RequestedAttributeResponseHandler
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

    def parse_attribute_update(self, payload: bytes) -> AttributeUpdate:
        pass

    def parse_rpc_request(self, topic: str, payload: bytes) -> RPCRequest:
        pass

    def parse_rpc_response(self, topic: str, payload: Union[bytes, Exception]) -> RPCResponse:
        pass

    def parse_provisioning_response(self, provisioning_request: ProvisioningRequest,
                                    payload: bytes) -> ProvisioningResponse:
        pass

    def __init__(self, response=None, exc=None):
        super().__init__()
        self._response = response
        self._exc = exc

    def parse_requested_attribute_response(self, topic, payload):
        if self._exc:
            raise self._exc
        return self._response


@pytest.mark.asyncio
async def test_set_message_adapter_and_full_handle_flow():
    handler = RequestedAttributeResponseHandler()
    # Build real AttributeRequest
    request = await AttributeRequest.build(shared_keys=["temp"], client_keys=["c1"])

    called = {}

    async def cb(resp):
        called["val"] = resp["temp"]

    # Register request
    await handler.register_request(request, cb)

    # Create real RequestedAttributeResponse with matching ID
    resp = RequestedAttributeResponse(
        request_id=request.request_id,
        shared=[AttributeEntry("temp", 42)],
        client=[AttributeEntry("c1", "v1")]
    )

    # Set adapter returning our response
    handler.set_message_adapter(DummyMessageAdapter(response=resp))

    # Handle message
    await handler.handle(f"some/topic/{request.request_id}", b"payload")

    assert called["val"] == 42
    assert request.request_id not in handler._pending_attribute_requests


def test_set_message_adapter_invalid_type():
    handler = RequestedAttributeResponseHandler()
    with pytest.raises(ValueError):
        handler.set_message_adapter(object())


@pytest.mark.asyncio
async def test_register_request_duplicate():
    handler = RequestedAttributeResponseHandler()
    handler.set_message_adapter(DummyMessageAdapter())

    req = await AttributeRequest.build()
    await handler.register_request(req, lambda r: None)
    with pytest.raises(RuntimeError):
        await handler.register_request(req, lambda r: None)


@pytest.mark.asyncio
async def test_unregister_existing_and_non_existing():
    handler = RequestedAttributeResponseHandler()
    handler.set_message_adapter(DummyMessageAdapter())

    req = await AttributeRequest.build()
    await handler.register_request(req, lambda r: None)

    assert req.request_id in handler._pending_attribute_requests
    handler.unregister_request(req.request_id)
    assert req.request_id not in handler._pending_attribute_requests

    # No error for missing ID
    handler.unregister_request(9999)


@pytest.mark.asyncio
async def test_handle_without_message_adapter_removes_request():
    handler = RequestedAttributeResponseHandler()
    req = await AttributeRequest.build()
    await handler.register_request(req, lambda r: None)
    await handler.handle(f"topic/{req.request_id}", b"payload")
    assert req.request_id not in handler._pending_attribute_requests


@pytest.mark.asyncio
async def test_handle_with_no_pending_request():
    handler = RequestedAttributeResponseHandler()
    resp = RequestedAttributeResponse(
        request_id=999,
        shared=[],
        client=[]
    )
    handler.set_message_adapter(DummyMessageAdapter(response=resp))
    # No request registered → should just return
    await handler.handle("topic/999", b"payload")


@pytest.mark.asyncio
async def test_handle_with_no_callback():
    handler = RequestedAttributeResponseHandler()
    req = await AttributeRequest.build()
    await handler.register_request(req, None)
    resp = RequestedAttributeResponse(
        request_id=req.request_id,
        shared=[],
        client=[]
    )
    handler.set_message_adapter(DummyMessageAdapter(response=resp))
    await handler.handle(f"topic/{req.request_id}", b"payload")


@pytest.mark.asyncio
async def test_handle_with_parse_exception():
    handler = RequestedAttributeResponseHandler()
    req = await AttributeRequest.build()
    await handler.register_request(req, lambda r: None)
    handler.set_message_adapter(DummyMessageAdapter(exc=ValueError("bad parse")))
    await handler.handle(f"topic/{req.request_id}", b"payload")


def test_clear():
    handler = RequestedAttributeResponseHandler()
    handler._pending_attribute_requests[1] = ("req", lambda r: None)
    handler.clear()
    assert not handler._pending_attribute_requests


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
