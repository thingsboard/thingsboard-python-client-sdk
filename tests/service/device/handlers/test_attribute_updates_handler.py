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
from typing import List, Union

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
from tb_mqtt_client.service.device.handlers.attribute_updates_handler import AttributeUpdatesHandler
from tb_mqtt_client.service.device.message_adapter import MessageAdapter


class DummyAdapter(MessageAdapter):
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

    def parse_rpc_request(self, topic: str, payload: bytes) -> RPCRequest:
        pass

    def parse_rpc_response(self, topic: str, payload: Union[bytes, Exception]) -> RPCResponse:
        pass

    def parse_provisioning_response(self, provisioning_request: ProvisioningRequest,
                                    payload: bytes) -> ProvisioningResponse:
        pass

    def build_uplink_messages(self, messages: List[MqttPublishMessage]) -> List[MqttPublishMessage]:
        pass

    def __init__(self, result=None, raise_exc=False):
        super().__init__()
        self.result = result
        self.raise_exc = raise_exc

    def parse_attribute_update(self, payload: bytes) -> AttributeUpdate:
        if self.raise_exc:
            raise ValueError("Simulated parse error")
        return self.result



@pytest.mark.asyncio
async def test_set_message_adapter_and_callback_called():
    handler = AttributeUpdatesHandler()
    adapter_result = AttributeUpdate([AttributeEntry("key", "value")])

    handler.set_message_adapter(DummyAdapter(result=adapter_result))

    called = {}

    async def cb(update: AttributeUpdate):
        called["data"] = update

    handler.set_callback(cb)

    await handler.handle("topic", b"payload")
    assert called["data"] == adapter_result


def test_set_message_adapter_invalid_type():
    handler = AttributeUpdatesHandler()
    with pytest.raises(ValueError) as exc:
        handler.set_message_adapter("not-a-message-adapter")
    assert "message_adapter must be an instance of MessageAdapter" in str(exc.value)


@pytest.mark.asyncio
async def test_handle_no_callback_set():
    handler = AttributeUpdatesHandler()
    # even if message_adapter is set, no callback means skip
    handler.set_message_adapter(DummyAdapter(result=AttributeUpdate({})))
    # should simply return without error
    await handler.handle("topic", b"payload")


@pytest.mark.asyncio
async def test_handle_parse_error():
    handler = AttributeUpdatesHandler()
    handler.set_message_adapter(DummyAdapter(raise_exc=True))

    called = {"called": False}

    async def cb(update: AttributeUpdate):
        called["called"] = True

    handler.set_callback(cb)
    # parse will raise, callback should not be called
    await handler.handle("topic", b"payload")
    assert not called["called"]


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
