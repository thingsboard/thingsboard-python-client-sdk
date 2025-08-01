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

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.direct_event_dispatcher import DirectEventDispatcher
from tb_mqtt_client.service.gateway.handlers.gateway_requested_attributes_response_handler import \
    GatewayRequestedAttributeResponseHandler
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter


@pytest.mark.asyncio
async def test_register_request():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)

    # Create a request
    request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])

    # Act
    await handler.register_request(request)

    # Assert
    assert (request.device_session.device_info.device_name, request.request_id) in handler._pending_attribute_requests
    assert handler._pending_attribute_requests[(request.device_session.device_info.device_name, request.request_id)][0] == request


@pytest.mark.asyncio
async def test_register_request_with_timeout():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)

    # Create a request
    request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])

    # Mock asyncio.get_event_loop().call_later
    with patch('asyncio.get_event_loop') as mock_get_loop:
        mock_loop = MagicMock()
        mock_get_loop.return_value = mock_loop
        mock_timeout_task = MagicMock()
        mock_loop.call_later.return_value = mock_timeout_task

        # Act
        await handler.register_request(request, timeout=10)

        # Assert
        assert (request.device_session.device_info.device_name, request.request_id) in handler._pending_attribute_requests
        assert handler._pending_attribute_requests[(request.device_session.device_info.device_name, request.request_id)][0] == request
        assert handler._pending_attribute_requests[(request.device_session.device_info.device_name, request.request_id)][1] == mock_timeout_task
        mock_loop.call_later.assert_called_once_with(10, handler._on_timeout, request.device_session.device_info.device_name, request.request_id)


@pytest.mark.asyncio
async def test_register_request_duplicate():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)

    # Create a request
    request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])

    # Register the request once
    await handler.register_request(request)

    # Act & Assert - should raise RuntimeError when registering the same request ID again
    with pytest.raises(RuntimeError):
        await handler.register_request(request)


@pytest.mark.asyncio
async def test_unregister_request():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)

    # Create a request
    request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])

    # Register the request
    handler._pending_attribute_requests[(request.device_session.device_info.device_name, request.request_id)] = (request, None)

    # Act
    handler.unregister_request(request.device_session.device_info.device_name, request.request_id)

    # Assert
    assert (request.device_session.device_info.device_name, request.request_id) not in handler._pending_attribute_requests


def test_unregister_nonexistent_request():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Act - should not raise an exception
    handler.unregister_request("nonexistent_device", 999)

    # Assert
    assert ("nonexistent_device", 999) not in handler._pending_attribute_requests


@pytest.mark.asyncio
async def test_handle_valid_response():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    device_manager.get_by_name.return_value = device_session

    # Create a request
    request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])

    # Register the request
    timeout_task = MagicMock()
    handler._pending_attribute_requests[(request.device_session.device_info.device_name, request.request_id)] = (request, timeout_task)

    # Mock the message adapter
    payload = b'{"device": "test_device", "id": ' + str(request.request_id).encode() + b', "values": {"key1": "value1"}}'
    deserialized_data = {"device": "test_device", "id": request.request_id, "values": {"key1": "value1"}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Create a mock response
    response = GatewayRequestedAttributeResponse(device_name="test_device", request_id=request.request_id, shared=[AttributeEntry("key1", "value1")])
    message_adapter.parse_gateway_requested_attribute_response.return_value = response

    # Mock asyncio.create_task
    with patch('asyncio.create_task') as mock_create_task:
        mock_task = MagicMock()
        mock_create_task.return_value = mock_task

        # Act
        await handler.handle("topic", payload)

        # Assert
        message_adapter.deserialize_to_dict.assert_called_once_with(payload)
        message_adapter.parse_gateway_requested_attribute_response.assert_called_once_with(request, deserialized_data)
        device_manager.get_by_name.assert_called_once_with("test_device")
        timeout_task.cancel.assert_called_once()
        mock_create_task.assert_called_once()
        mock_task.add_done_callback.assert_called_once_with(handler._handle_callback_exception)


@pytest.mark.asyncio
async def test_handle_missing_fields():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Mock the message adapter
    payload = b'{"values": {"key1": "value1"}}'  # Missing 'device' and 'id'
    deserialized_data = {"values": {"key1": "value1"}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Act
    await handler.handle("topic", payload)

    # Assert
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)


@pytest.mark.asyncio
async def test_handle_no_pending_request():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Mock the message adapter
    payload = b'{"device": "test_device", "id": 999, "values": {"key1": "value1"}}'
    deserialized_data = {"device": "test_device", "id": 999, "values": {"key1": "value1"}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data

    # Act
    result = await handler.handle("topic", payload)

    # Assert
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)
    assert result is None


@pytest.mark.asyncio
async def test_on_timeout():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)

    # Create a request
    request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])

    # Register the request
    handler._pending_attribute_requests[(request.device_session.device_info.device_name, request.request_id)] = (request, None)

    handler._on_timeout(request.device_session.device_info.device_name, request.request_id)

    # Assert
    assert (request.device_session.device_info.device_name, request.request_id) not in handler._pending_attribute_requests


def test_handle_callback_exception():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a mock task that raises an exception
    task = MagicMock(spec=asyncio.Task)
    task.result.side_effect = Exception("Test exception")

    # Act
    handler._handle_callback_exception(task)

    # Assert
    task.result.assert_called_once()


@pytest.mark.asyncio
async def test_clear():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)

    # Create a handler
    handler = GatewayRequestedAttributeResponseHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )

    # Create a device session
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)

    # Create a request
    request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])

    # Register the request
    handler._pending_attribute_requests[(request.device_session.device_info.device_name, request.request_id)] = (request, None)

    # Act
    handler.clear()

    # Assert
    assert len(handler._pending_attribute_requests) == 0

@pytest.mark.asyncio
async def test_handle_no_message_adapter_removes_request():
    adapter = MagicMock(spec=GatewayMessageAdapter)
    handler = GatewayRequestedAttributeResponseHandler(event_dispatcher=MagicMock(spec=DirectEventDispatcher),
                                                      message_adapter=adapter,
                                                      device_manager=MagicMock(spec=DeviceManager))
    handler._pending_attribute_requests["test_device", 5] = (MagicMock(spec=AttributeRequest), AsyncMock())
    topic = "attr/request/5"
    await handler.handle(topic, b"{}")
    assert 5 not in handler._pending_attribute_requests

@pytest.mark.asyncio
async def test_handle_with_no_callback_registered():
    adapter = MagicMock(spec=GatewayMessageAdapter)
    handler = GatewayRequestedAttributeResponseHandler(event_dispatcher=MagicMock(spec=DirectEventDispatcher),
                                                      message_adapter=adapter,
                                                      device_manager=MagicMock(spec=DeviceManager))
    resp = MagicMock(spec=RequestedAttributeResponse)
    resp.request_id = 42
    adapter.parse_gateway_requested_attribute_response.return_value = resp
    handler._pending_attribute_requests['test_device', 42] = (MagicMock(spec=AttributeRequest), None)
    await handler.handle("topic", b"payload")

@pytest.mark.asyncio
async def test_handle_with_parsing_exception():
    adapter = MagicMock(spec=GatewayMessageAdapter)
    handler = GatewayRequestedAttributeResponseHandler(event_dispatcher=MagicMock(spec=DirectEventDispatcher),
                                                      message_adapter=adapter,
                                                      device_manager=MagicMock(spec=DeviceManager))
    adapter.parse_gateway_requested_attribute_response.side_effect = RuntimeError("bad parse")
    await handler.handle("topic", b"payload")


if __name__ == '__main__':
    pytest.main([__file__])
