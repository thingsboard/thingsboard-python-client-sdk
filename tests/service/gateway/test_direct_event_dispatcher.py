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

from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_event import GatewayEvent
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.direct_event_dispatcher import DirectEventDispatcher


def test_init():
    # Setup & Act
    dispatcher = DirectEventDispatcher()

    # Assert
    assert isinstance(dispatcher._handlers, dict)
    assert len(dispatcher._handlers) == 0
    assert isinstance(dispatcher._lock, asyncio.Lock)


def test_register():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Act
    dispatcher.register(event_type, callback)

    # Assert
    assert event_type in dispatcher._handlers
    assert callback in dispatcher._handlers[event_type]
    assert len(dispatcher._handlers[event_type]) == 1


def test_register_duplicate():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register the callback twice
    dispatcher.register(event_type, callback)
    dispatcher.register(event_type, callback)

    # Assert
    assert event_type in dispatcher._handlers
    assert callback in dispatcher._handlers[event_type]
    assert len(dispatcher._handlers[event_type]) == 1  # Should only be added once


def test_register_multiple():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback1 = MagicMock()
    callback2 = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Act
    dispatcher.register(event_type, callback1)
    dispatcher.register(event_type, callback2)

    # Assert
    assert event_type in dispatcher._handlers
    assert callback1 in dispatcher._handlers[event_type]
    assert callback2 in dispatcher._handlers[event_type]
    assert len(dispatcher._handlers[event_type]) == 2


def test_unregister():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register and then unregister
    dispatcher.register(event_type, callback)
    dispatcher.unregister(event_type, callback)

    # Assert
    assert event_type not in dispatcher._handlers  # Event type should be removed when last callback is unregistered


def test_unregister_nonexistent():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Act - should not raise an exception
    dispatcher.unregister(event_type, callback)

    # Assert
    assert event_type not in dispatcher._handlers


def test_unregister_one_of_many():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback1 = MagicMock()
    callback2 = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register both callbacks and unregister one
    dispatcher.register(event_type, callback1)
    dispatcher.register(event_type, callback2)
    dispatcher.unregister(event_type, callback1)

    # Assert
    assert event_type in dispatcher._handlers
    assert callback1 not in dispatcher._handlers[event_type]
    assert callback2 in dispatcher._handlers[event_type]
    assert len(dispatcher._handlers[event_type]) == 1


@pytest.mark.asyncio
async def test_dispatch_to_device_session():
    # Setup
    dispatcher = DirectEventDispatcher()
    device_session = MagicMock(spec=DeviceSession)
    device_session.handle_event_to_device = AsyncMock()

    # Create a mock event
    event = MagicMock(spec=GatewayEvent)
    event.event_type = GatewayEventType.DEVICE_ATTRIBUTE_UPDATE

    # Act
    await dispatcher.dispatch(event, device_session=device_session)

    # Assert
    device_session.handle_event_to_device.assert_awaited_once_with(event)


@pytest.mark.asyncio
async def test_dispatch_to_sync_callback():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register the callback
    dispatcher.register(event_type, callback)

    # Create a mock event
    event = MagicMock(spec=GatewayEvent)
    event.event_type = event_type

    # Act
    result = await dispatcher.dispatch(event)

    # Assert
    callback.assert_called_once_with(event)
    assert result == callback.return_value


@pytest.mark.asyncio
async def test_dispatch_to_async_callback():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = AsyncMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register the callback
    dispatcher.register(event_type, callback)

    # Create a mock event
    event = MagicMock(spec=GatewayEvent)
    event.event_type = event_type

    # Act
    result = await dispatcher.dispatch(event)

    # Assert
    callback.assert_awaited_once_with(event)
    assert result == callback.return_value


@pytest.mark.asyncio
async def test_dispatch_with_args():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = MagicMock()
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register the callback
    dispatcher.register(event_type, callback)

    # Create a mock event
    event = MagicMock(spec=GatewayEvent)
    event.event_type = event_type

    # Act
    await dispatcher.dispatch(event, "arg1", "arg2", kwarg1="value1", kwarg2="value2")

    # Assert
    callback.assert_called_once_with(event, "arg1", "arg2", kwarg1="value1", kwarg2="value2")


@pytest.mark.asyncio
async def test_dispatch_no_handlers():
    # Setup
    dispatcher = DirectEventDispatcher()

    # Create a mock event
    event = MagicMock(spec=GatewayEvent)
    event.event_type = GatewayEventType.DEVICE_CONNECT

    # Act
    result = await dispatcher.dispatch(event)

    # Assert
    assert result is None


@pytest.mark.asyncio
async def test_dispatch_callback_exception():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = MagicMock(side_effect=Exception("Test exception"))
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register the callback
    dispatcher.register(event_type, callback)

    # Create a mock event
    event = MagicMock(spec=GatewayEvent)
    event.event_type = event_type

    # Act
    result = await dispatcher.dispatch(event)

    # Assert
    callback.assert_called_once_with(event)
    assert result is None


@pytest.mark.asyncio
async def test_dispatch_async_callback_exception():
    # Setup
    dispatcher = DirectEventDispatcher()
    callback = AsyncMock(side_effect=Exception("Test exception"))
    event_type = GatewayEventType.DEVICE_CONNECT

    # Register the callback
    dispatcher.register(event_type, callback)

    # Create a mock event
    event = MagicMock(spec=GatewayEvent)
    event.event_type = event_type

    # Act
    result = await dispatcher.dispatch(event)

    # Assert
    callback.assert_awaited_once_with(event)
    assert result is None


if __name__ == '__main__':
    pytest.main([__file__])
