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
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.direct_event_dispatcher import DirectEventDispatcher
from tb_mqtt_client.service.gateway.handlers.gateway_rpc_handler import GatewayRPCHandler
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter


def test_init():
    # Setup
    event_dispatcher = MagicMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Act
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Assert
    assert handler._event_dispatcher == event_dispatcher
    assert handler._message_adapter == message_adapter
    assert handler._device_manager == device_manager
    assert handler._stop_event == stop_event
    assert handler._callback is None
    event_dispatcher.register.assert_called_once_with(GatewayEventType.DEVICE_RPC_REQUEST, handler.handle)


@pytest.mark.asyncio
async def test_handle_successful_rpc():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    device_manager.get_by_name.return_value = device_session

    # Mock the message adapter
    topic = "v1/gateway/rpc"
    payload = b'{"device": "test_device", "id": 1, "method": "test_method", "params": {"param1": "value1"}}'
    deserialized_data = {"device": "test_device",
                         "data": {"id": 1, "method": "test_method", "params": {"param1": "value1"}}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Create a mock RPC request
    rpc_request = GatewayRPCRequest._deserialize_from_dict(deserialized_data)
    message_adapter.parse_rpc_request.return_value = rpc_request

    # Create a mock RPC response
    rpc_response = GatewayRPCResponse.build(device_name="test_device", request_id=1, result="success")

    # Mock the event dispatcher to return the RPC response
    event_dispatcher.dispatch.side_effect = [rpc_response, asyncio.Future()]

    # Mock await_or_stop
    with patch('tb_mqtt_client.service.gateway.handlers.gateway_rpc_handler.await_or_stop') as mock_await_or_stop:
        # Act
        await handler.handle(topic, payload)

        # Assert
        message_adapter.deserialize_to_dict.assert_called_once_with(payload)
        message_adapter.parse_rpc_request.assert_called_once_with(topic, deserialized_data)
        device_manager.get_by_name.assert_called_once_with("test_device")

        # Check that dispatch was called twice - once for the request and once for the response
        assert event_dispatcher.dispatch.call_count == 2
        event_dispatcher.dispatch.assert_any_call(rpc_request, device_session=device_session)
        event_dispatcher.dispatch.assert_any_call(rpc_response)

        mock_await_or_stop.assert_called_once()


@pytest.mark.asyncio
async def test_handle_no_message_adapter():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler with no message adapter
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=None,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Act
    result = await handler.handle("topic", b'{}')

    # Assert
    assert result is None


@pytest.mark.asyncio
async def test_handle_no_device_session():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Mock the device manager to return None (no device session)
    device_manager.get_by_name.return_value = None

    # Mock the message adapter
    topic = "v1/gateway/rpc"
    payload = b'{"device": "nonexistent_device", "id": 1, "method": "test_method", "params": {"param1": "value1"}}'
    deserialized_data = {"device": "nonexistent_device",
                         "data": {"id": 1, "method": "test_method", "params": {"param1": "value1"}}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Create a mock RPC request
    rpc_request = GatewayRPCRequest._deserialize_from_dict(deserialized_data)
    message_adapter.parse_rpc_request.return_value = rpc_request

    # Act
    result = await handler.handle(topic, payload)

    # Assert
    assert result is None
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)
    message_adapter.parse_rpc_request.assert_called_once_with(topic, deserialized_data)
    device_manager.get_by_name.assert_called_once_with("nonexistent_device")


@pytest.mark.asyncio
async def test_handle_no_response_from_callback():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    device_manager.get_by_name.return_value = device_session

    # Mock the message adapter
    topic = "v1/gateway/rpc"
    payload = b'{"device": "test_device", "id": 1, "method": "test_method", "params": {"param1": "value1"}}'
    deserialized_data = {"device": "test_device",
                         "data": {"id": 1, "method": "test_method", "params": {"param1": "value1"}}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Create a mock RPC request
    rpc_request = GatewayRPCRequest._deserialize_from_dict(deserialized_data)
    message_adapter.parse_rpc_request.return_value = rpc_request

    # Mock the event dispatcher to return None (no response from callback)
    event_dispatcher.dispatch.return_value = None

    # Act
    result = await handler.handle(topic, payload)

    # Assert
    assert result is None
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)
    message_adapter.parse_rpc_request.assert_called_once_with(topic, deserialized_data)
    device_manager.get_by_name.assert_called_once_with("test_device")
    event_dispatcher.dispatch.assert_called_once_with(rpc_request, device_session=device_session)


@pytest.mark.asyncio
async def test_handle_invalid_response_type():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    device_manager.get_by_name.return_value = device_session

    # Mock the message adapter
    topic = "v1/gateway/rpc"
    payload = b'{"device": "test_device", "id": 1, "method": "test_method", "params": {"param1": "value1"}}'
    deserialized_data = {"device": "test_device",
                         "data": {"id": 1, "method": "test_method", "params": {"param1": "value1"}}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Create a mock RPC request
    rpc_request = GatewayRPCRequest._deserialize_from_dict(deserialized_data)
    message_adapter.parse_rpc_request.return_value = rpc_request

    # Mock the event dispatcher to return an invalid response type
    event_dispatcher.dispatch.side_effect = ["invalid_response", asyncio.Future()]

    # Act
    await handler.handle(topic, payload)

    # Assert
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)
    message_adapter.parse_rpc_request.assert_called_once_with(topic, deserialized_data)
    device_manager.get_by_name.assert_called_once_with("test_device")
    event_dispatcher.dispatch.assert_called()


@pytest.mark.asyncio
async def test_handle_exception_in_processing():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    device_manager.get_by_name.return_value = device_session

    # Mock the message adapter to raise an exception
    topic = "v1/gateway/rpc"
    payload = b'{"device": "test_device", "id": 1, "method": "test_method", "params": {"param1": "value1"}}'
    message_adapter.deserialize_to_dict.side_effect = Exception("Test exception")

    # Act
    result = await handler.handle(topic, payload)

    # Assert
    assert result is None
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)


@pytest.mark.asyncio
async def test_handle_timeout_in_publish():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    device_manager.get_by_name.return_value = device_session

    # Mock the message adapter
    topic = "v1/gateway/rpc"
    payload = b'{"device": "test_device", "id": 1, "method": "test_method", "params": {"param1": "value1"}}'
    deserialized_data = {"device": "test_device",
                         "data": {"id": 1, "method": "test_method", "params": {"param1": "value1"}}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Create a mock RPC request
    rpc_request = GatewayRPCRequest._deserialize_from_dict(deserialized_data)
    message_adapter.parse_rpc_request.return_value = rpc_request

    # Create a mock RPC response
    rpc_response = GatewayRPCResponse.build(device_name="test_device", request_id=1, result="success")

    # Mock the event dispatcher
    event_dispatcher.dispatch.side_effect = [rpc_response, asyncio.Future()]

    # Mock await_or_stop to raise TimeoutError
    with patch('tb_mqtt_client.service.gateway.handlers.gateway_rpc_handler.await_or_stop') as mock_await_or_stop:
        mock_await_or_stop.side_effect = TimeoutError("Timeout")

        # Act
        await handler.handle(topic, payload)

        # Assert
        message_adapter.deserialize_to_dict.assert_called_once_with(payload)
        message_adapter.parse_rpc_request.assert_called_once_with(topic, deserialized_data)
        device_manager.get_by_name.assert_called_once_with("test_device")
        event_dispatcher.dispatch.assert_any_call(rpc_request, device_session=device_session)
        event_dispatcher.dispatch.assert_any_call(rpc_response)
        mock_await_or_stop.assert_called_once()


@pytest.mark.asyncio
async def test_handle_no_publish_futures():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    stop_event = asyncio.Event()

    # Create a handler
    handler = GatewayRPCHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager,
        stop_event=stop_event
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    device_manager.get_by_name.return_value = device_session

    # Mock the message adapter
    topic = "v1/gateway/rpc"
    payload = b'{"device": "test_device", "id": 1, "method": "test_method", "params": {"param1": "value1"}}'
    deserialized_data = {"device": "test_device",
                         "data": {"id": 1, "method": "test_method", "params": {"param1": "value1"}}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Create a mock RPC request
    rpc_request = GatewayRPCRequest._deserialize_from_dict(deserialized_data)
    message_adapter.parse_rpc_request.return_value = rpc_request

    # Create a mock RPC response
    rpc_response = GatewayRPCResponse.build(device_name="test_device", request_id=1, result="success")

    # Mock the event dispatcher to return the RPC response but no publish futures
    event_dispatcher.dispatch.side_effect = [rpc_response, None]

    # Act
    result = await handler.handle(topic, payload)

    # Assert
    assert result is None
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)
    message_adapter.parse_rpc_request.assert_called_once_with(topic, deserialized_data)
    device_manager.get_by_name.assert_called_once_with("test_device")
    event_dispatcher.dispatch.assert_any_call(rpc_request, device_session=device_session)
    event_dispatcher.dispatch.assert_any_call(rpc_response)


if __name__ == '__main__':
    pytest.main([__file__])
