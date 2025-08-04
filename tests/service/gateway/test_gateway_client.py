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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.constants.mqtt_topics import GATEWAY_CONNECT_TOPIC, GATEWAY_DISCONNECT_TOPIC, \
    GATEWAY_TELEMETRY_TOPIC, GATEWAY_ATTRIBUTES_TOPIC, GATEWAY_ATTRIBUTES_REQUEST_TOPIC, GATEWAY_CLAIM_TOPIC
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_claim_request import GatewayClaimRequestBuilder
from tb_mqtt_client.service.gateway.client import GatewayClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession


# @pytest.mark.asyncio
# async def test_connect():
#     # Setup
#     client = GatewayClient()
#     client._mqtt_manager = AsyncMock()
#     client._mqtt_manager.is_connected = MagicMock(return_value=True)
#     client._mqtt_manager.await_ready = AsyncMock()
#     client._mqtt_manager.subscribe = AsyncMock(return_value=asyncio.Future())
#     client._mqtt_manager.register_handler = MagicMock()
#
#     # Act
#     await client.connect()
#
#     # Assert
#     client._mqtt_manager.connect.assert_awaited_once()
#     assert client._mqtt_manager.subscribe.call_count == 3  # Should subscribe to 3 topics
#     assert client._mqtt_manager.register_handler.call_count == 3  # Should register 3 handlers


@pytest.mark.asyncio
async def test_connect_device():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    client._event_dispatcher.dispatch = AsyncMock()
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_CONNECT_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    device_session, result = await client.connect_device("test_device", wait_for_publish=True)
    
    # Assert
    assert device_session is not None
    assert device_session.device_info.device_name == "test_device"
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_CONNECT_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_device_with_connect_message():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    client._event_dispatcher.dispatch = AsyncMock()
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_CONNECT_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Create a device connect message
    connect_message = DeviceConnectMessage.build("test_device", "custom_profile")
    
    # Act
    device_session, result = await client.connect_device(connect_message, wait_for_publish=True)
    
    # Assert
    assert device_session is not None
    assert device_session.device_info.device_name == "test_device"
    assert device_session.device_info.device_profile == "custom_profile"
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_CONNECT_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_device_no_wait():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    device_session, futures = await client.connect_device("test_device", wait_for_publish=False)
    
    # Assert
    assert device_session is not None
    assert device_session.device_info.device_name == "test_device"
    assert isinstance(futures[0], asyncio.Future)
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_disconnect_device():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_DISCONNECT_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    session, result = await client.disconnect_device(device_session, wait_for_publish=True)
    
    # Assert
    assert session is not None
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_DISCONNECT_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_disconnect_device_no_wait():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    session, futures = await client.disconnect_device(device_session, wait_for_publish=False)
    
    # Assert
    assert session is not None
    assert isinstance(futures[0], asyncio.Future)
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_timeseries():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_TELEMETRY_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    result = await client.send_device_timeseries(device_session, {"temperature": 25.5}, wait_for_publish=True)
    
    # Assert
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_TELEMETRY_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_timeseries_with_timeseries_entry():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_TELEMETRY_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Create a timeseries entry
    timeseries = TimeseriesEntry("temperature", 25.5)
    
    # Act
    result = await client.send_device_timeseries(device_session, timeseries, wait_for_publish=True)
    
    # Assert
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_TELEMETRY_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_timeseries_no_wait():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    future = await client.send_device_timeseries(device_session, {"temperature": 25.5}, wait_for_publish=False)
    
    # Assert
    assert isinstance(future, asyncio.Future)
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_attributes():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_ATTRIBUTES_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    result = await client.send_device_attributes(device_session, {"firmware_version": "1.0.0"}, wait_for_publish=True)
    
    # Assert
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_ATTRIBUTES_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_attributes_with_attribute_entry():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_ATTRIBUTES_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Create an attribute entry
    attributes = AttributeEntry("firmware_version", "1.0.0")
    
    # Act
    result = await client.send_device_attributes(device_session, attributes, wait_for_publish=True)
    
    # Assert
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_ATTRIBUTES_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_attributes_no_wait():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Act
    future = await client.send_device_attributes(device_session, {"firmware_version": "1.0.0"}, wait_for_publish=False)
    
    # Assert
    assert isinstance(future, asyncio.Future)
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_attributes_request():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    client._gateway_requested_attribute_response_handler = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_ATTRIBUTES_REQUEST_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Create a gateway attribute request
    request = await GatewayAttributeRequest.build(device_session, shared_keys=["firmware_version"], client_keys=None)
    
    # Act
    result = await client.send_device_attributes_request(device_session, request, wait_for_publish=True)
    
    # Assert
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_ATTRIBUTES_REQUEST_TOPIC
    client._gateway_requested_attribute_response_handler.register_request.assert_awaited_once()
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_attributes_request_no_wait():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    client._gateway_requested_attribute_response_handler = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that will be returned by dispatch
    future = asyncio.Future()
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Create a gateway attribute request
    request = await GatewayAttributeRequest.build(device_session, shared_keys=["firmware_version"], client_keys=None)
    
    # Act
    future = await client.send_device_attributes_request(device_session, request, wait_for_publish=False)
    
    # Assert
    assert isinstance(future, asyncio.Future)
    client._gateway_requested_attribute_response_handler.register_request.assert_awaited_once()
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_claim_request():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that dispatch will return
    future = asyncio.Future()
    future.set_result(PublishResult(topic=GATEWAY_CLAIM_TOPIC, qos=1, message_id=1, payload_size=100, reason_code=0))
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Create a gateway claim request
    device_claim_request = ClaimRequest.build(secret_key="secret", duration=1000)
    gateway_claim_request = GatewayClaimRequestBuilder().add_device_request(device_session, device_claim_request).build()
    
    # Act
    result = await client.send_device_claim_request(device_session, gateway_claim_request, wait_for_publish=True)
    
    # Assert
    assert isinstance(result, PublishResult)
    assert result.topic == GATEWAY_CLAIM_TOPIC
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_device_claim_request_no_wait():
    # Setup
    client = GatewayClient()
    client._event_dispatcher = AsyncMock()
    
    # Create a device session
    
    info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info=info)
    
    # Create a future that dispatch will return
    future = asyncio.Future()
    client._event_dispatcher.dispatch.return_value = [future]
    
    # Create a gateway claim request
    device_claim_request = ClaimRequest.build(secret_key="secret", duration=1000)
    gateway_claim_request = GatewayClaimRequestBuilder().add_device_request(device_session, device_claim_request).build()
    
    # Act
    future = await client.send_device_claim_request(device_session, gateway_claim_request, wait_for_publish=False)
    
    # Assert
    assert isinstance(future, asyncio.Future)
    client._event_dispatcher.dispatch.assert_awaited_once()


@pytest.mark.asyncio
async def test_disconnect():
    # Setup
    client = GatewayClient()
    client._mqtt_manager = AsyncMock()
    client._mqtt_manager.unsubscribe = AsyncMock(return_value=asyncio.Future())
    
    # Act
    await client.disconnect()
    
    # Assert
    client._mqtt_manager.unsubscribe.assert_awaited()
    client._mqtt_manager.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_rate_limit_response():
    # Setup
    client = GatewayClient()
    client._mqtt_manager = MagicMock()
    client._gateway_message_adapter = MagicMock()
    client._gateway_message_adapter.splitter = MagicMock()
    client._gateway_rate_limiter = MagicMock()
    client._gateway_rate_limiter.message_rate_limit = AsyncMock()
    client._gateway_rate_limiter.telemetry_message_rate_limit = AsyncMock()
    client._gateway_rate_limiter.telemetry_datapoints_rate_limit = AsyncMock()
    
    # Create a response with gateway rate limits
    response = RPCResponse.build(1, result={
        'gatewayRateLimits': {
            'messages': '10:1,',
            'telemetryMessages': '100:60,',
            'telemetryDataPoints': '500:60,'
        },
        'maxPayloadSize': 512
    })
    
    # Mock the parent class method
    with patch('tb_mqtt_client.service.device.client.DeviceClient._handle_rate_limit_response', return_value=True):
        # Act
        result = await client._handle_rate_limit_response(response)
        
        # Assert
        assert result is True
        client._gateway_rate_limiter.message_rate_limit.set_limit.assert_awaited_once()
        client._gateway_rate_limiter.telemetry_message_rate_limit.set_limit.assert_awaited_once()
        client._gateway_rate_limiter.telemetry_datapoints_rate_limit.set_limit.assert_awaited_once()
        client._mqtt_manager.set_gateway_rate_limits_received.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__])
