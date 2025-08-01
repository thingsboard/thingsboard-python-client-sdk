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
from unittest.mock import MagicMock, AsyncMock

import pytest

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.constants.mqtt_topics import GATEWAY_TELEMETRY_TOPIC, GATEWAY_ATTRIBUTES_TOPIC, \
    GATEWAY_CONNECT_TOPIC, GATEWAY_DISCONNECT_TOPIC, GATEWAY_ATTRIBUTES_REQUEST_TOPIC, GATEWAY_RPC_TOPIC, \
    GATEWAY_CLAIM_TOPIC
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.device_disconnect_message import DeviceDisconnectMessage
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_claim_request import GatewayClaimRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessageBuilder
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter
from tb_mqtt_client.service.gateway.message_sender import GatewayMessageSender
from tb_mqtt_client.service.message_service import MessageService


def test_init():
    # Setup & Act
    sender = GatewayMessageSender()
    
    # Assert
    assert sender._message_queue is None
    assert sender._message_adapter is None


def test_set_message_queue():
    # Setup
    sender = GatewayMessageSender()
    message_queue = MagicMock(spec=MessageService)
    
    # Act
    sender.set_message_queue(message_queue)
    
    # Assert
    assert sender._message_queue == message_queue


def test_set_message_adapter():
    # Setup
    sender = GatewayMessageSender()
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    
    # Act
    sender.set_message_adapter(message_adapter)
    
    # Assert
    assert sender._message_adapter == message_adapter


@pytest.mark.asyncio
async def test_send_uplink_message_with_timeseries():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    sender.set_message_queue(message_queue)
    
    # Create an uplink message with timeseries
    uplink_message = (GatewayUplinkMessageBuilder()
                      .set_device_name("test_device")
                      .add_timeseries([TimeseriesEntry("temp", 22.5)])
                      .build())
    
    # Mock the publish method to set delivery_futures
    async def mock_publish(mqtt_message):
        mqtt_message.delivery_futures = [asyncio.Future()]
        return [mqtt_message]
    
    message_queue.publish.side_effect = mock_publish
    
    # Act
    result = await sender.send_uplink_message(uplink_message)
    
    # Assert
    assert result is not None
    assert len(result) == 1
    assert isinstance(result[0], asyncio.Future)
    message_queue.publish.assert_called_once()
    
    # Verify the MQTT message
    mqtt_message = message_queue.publish.call_args[0][0]
    assert mqtt_message.topic == GATEWAY_TELEMETRY_TOPIC
    assert mqtt_message.payload == uplink_message
    assert mqtt_message.qos == 1


@pytest.mark.asyncio
async def test_send_uplink_message_with_attributes():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    sender.set_message_queue(message_queue)
    
    # Create an uplink message with attributes
    uplink_message = (GatewayUplinkMessageBuilder()
                      .set_device_name("test_device")
                      .add_attributes([AttributeEntry("temperature", 22.5)])
                      .build())
    
    # Mock the publish method to set delivery_futures
    async def mock_publish(mqtt_message):
        mqtt_message.delivery_futures = [asyncio.Future()]
        return [mqtt_message]
    
    message_queue.publish.side_effect = mock_publish
    
    # Act
    result = await sender.send_uplink_message(uplink_message)
    
    # Assert
    assert result is not None
    assert len(result) == 1
    assert isinstance(result[0], asyncio.Future)
    message_queue.publish.assert_called_once()
    
    # Verify the MQTT message
    mqtt_message = message_queue.publish.call_args[0][0]
    assert mqtt_message.topic == GATEWAY_ATTRIBUTES_TOPIC
    assert mqtt_message.payload == uplink_message
    assert mqtt_message.qos == 1


@pytest.mark.asyncio
async def test_send_uplink_message_with_both():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    sender.set_message_queue(message_queue)
    
    # Create an uplink message with both timeseries and attributes
    uplink_message = (GatewayUplinkMessageBuilder()
                      .set_device_name("test_device")
                      .add_timeseries([TimeseriesEntry("temp", 22.5)])
                      .add_attributes(AttributeEntry("key1", "value1"))
                      .build())
    
    # Mock the publish method to set delivery_futures
    async def mock_publish(mqtt_message):
        mqtt_message.delivery_futures = [asyncio.Future()]
        return [mqtt_message]
    
    message_queue.publish.side_effect = mock_publish
    
    # Act
    result = await sender.send_uplink_message(uplink_message)
    
    # Assert
    assert result is not None
    assert len(result) == 2  # One for telemetry, one for attributes
    assert isinstance(result[0], asyncio.Future)
    assert isinstance(result[1], asyncio.Future)
    assert message_queue.publish.call_count == 2
    
    # Verify the MQTT messages
    call_args_list = message_queue.publish.call_args_list
    assert call_args_list[0][0][0].topic == GATEWAY_TELEMETRY_TOPIC
    assert call_args_list[0][0][0].payload == uplink_message
    assert call_args_list[1][0][0].topic == GATEWAY_ATTRIBUTES_TOPIC
    assert call_args_list[1][0][0].payload == uplink_message


@pytest.mark.asyncio
async def test_send_device_connect():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    sender.set_message_queue(message_queue)
    sender.set_message_adapter(message_adapter)
    
    # Create a device connect message
    device_connect_message = DeviceConnectMessage.build("test_device", "default")
    
    # Mock the message adapter
    mqtt_message = MqttPublishMessage(GATEWAY_CONNECT_TOPIC, b'{"device":"test_device","type":"default"}', qos=1)
    mqtt_message.delivery_futures = [asyncio.Future()]
    message_adapter.build_device_connect_message_payload.return_value = mqtt_message
    
    # Act
    result = await sender.send_device_connect(device_connect_message)
    
    # Assert
    assert result is not None
    assert len(result) == 1
    assert isinstance(result[0], asyncio.Future)
    message_adapter.build_device_connect_message_payload.assert_called_once_with(device_connect_message=device_connect_message, qos=1)
    message_queue.publish.assert_called_once_with(mqtt_message)


@pytest.mark.asyncio
async def test_send_device_disconnect():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    sender.set_message_queue(message_queue)
    sender.set_message_adapter(message_adapter)
    
    # Create a device disconnect message
    device_disconnect_message = DeviceDisconnectMessage.build("test_device")
    
    # Mock the message adapter
    mqtt_message = MqttPublishMessage(GATEWAY_DISCONNECT_TOPIC, b'{"device":"test_device"}', qos=1)
    mqtt_message.delivery_futures = [asyncio.Future()]
    message_adapter.build_device_disconnect_message_payload.return_value = mqtt_message
    
    # Act
    result = await sender.send_device_disconnect(device_disconnect_message)
    
    # Assert
    assert result is not None
    assert len(result) == 1
    assert isinstance(result[0], asyncio.Future)
    message_adapter.build_device_disconnect_message_payload.assert_called_once_with(device_disconnect_message=device_disconnect_message, qos=1)
    message_queue.publish.assert_called_once_with(mqtt_message)


@pytest.mark.asyncio
async def test_send_attributes_request():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    sender.set_message_queue(message_queue)
    sender.set_message_adapter(message_adapter)
    
    # Create an attribute request
    attribute_request = MagicMock(spec=GatewayAttributeRequest)
    
    # Mock the message adapter
    mqtt_message = MqttPublishMessage(GATEWAY_ATTRIBUTES_REQUEST_TOPIC, b'{"device":"test_device","keys":["key1"]}', qos=1)
    mqtt_message.delivery_futures = [asyncio.Future()]
    message_adapter.build_gateway_attribute_request_payload.return_value = mqtt_message
    
    # Act
    result = await sender.send_attributes_request(attribute_request)
    
    # Assert
    assert result is not None
    assert len(result) == 1
    assert isinstance(result[0], asyncio.Future)
    message_adapter.build_gateway_attribute_request_payload.assert_called_once_with(attribute_request=attribute_request, qos=1)
    message_queue.publish.assert_called_once_with(mqtt_message)


@pytest.mark.asyncio
async def test_send_rpc_response():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    sender.set_message_queue(message_queue)
    sender.set_message_adapter(message_adapter)
    
    # Create an RPC response
    rpc_response = MagicMock(spec=GatewayRPCResponse)
    
    # Mock the message adapter
    mqtt_message = MqttPublishMessage(GATEWAY_RPC_TOPIC, b'{"device":"test_device","id":1,"data":{"result":"success"}}', qos=1)
    mqtt_message.delivery_futures = [asyncio.Future()]
    message_adapter.build_rpc_response_payload.return_value = mqtt_message
    
    # Act
    result = await sender.send_rpc_response(rpc_response)
    
    # Assert
    assert result is not None
    assert len(result) == 1
    assert isinstance(result[0], asyncio.Future)
    message_adapter.build_rpc_response_payload.assert_called_once_with(rpc_response=rpc_response, qos=1)
    message_queue.publish.assert_called_once_with(mqtt_message)


@pytest.mark.asyncio
async def test_send_claim_request():
    # Setup
    sender = GatewayMessageSender()
    message_queue = AsyncMock(spec=MessageService)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    sender.set_message_queue(message_queue)
    sender.set_message_adapter(message_adapter)
    
    # Create a claim request
    claim_request = MagicMock(spec=GatewayClaimRequest)
    
    # Mock the message adapter
    mqtt_message = MqttPublishMessage(GATEWAY_CLAIM_TOPIC, b'{"device":"test_device","secretKey":"secret"}', qos=1)
    mqtt_message.delivery_futures = [asyncio.Future()]
    message_adapter.build_claim_request_payload.return_value = mqtt_message
    
    # Act
    result = await sender.send_claim_request(claim_request)
    
    # Assert
    assert result is not None
    assert len(result) == 1
    assert isinstance(result[0], asyncio.Future)
    message_adapter.build_claim_request_payload.assert_called_once_with(claim_request=claim_request, qos=1)
    message_queue.publish.assert_called_once_with(mqtt_message)


if __name__ == '__main__':
    pytest.main([__file__])