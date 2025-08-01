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


from unittest.mock import MagicMock, AsyncMock

import pytest

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.gateway.gateway_attribute_update import GatewayAttributeUpdate
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.direct_event_dispatcher import DirectEventDispatcher
from tb_mqtt_client.service.gateway.handlers.gateway_attribute_updates_handler import GatewayAttributeUpdatesHandler
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter


@pytest.mark.asyncio
async def test_handle_with_existing_device():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    
    # Create a handler
    handler = GatewayAttributeUpdatesHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )
    
    # Mock the device session
    device_session = MagicMock(spec=DeviceSession)
    device_manager.get_by_name.return_value = device_session
    
    # Mock the message adapter
    payload = b'{"device": "test_device", "data": {"key": "value"}}'
    deserialized_data = {"device": "test_device", "data": {"key": "value"}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data
    
    # Create a mock attribute update
    attribute_update = AttributeUpdate([AttributeEntry(key="key", value="value")])
    gateway_attribute_update = GatewayAttributeUpdate(device_name="test_device", attribute_update=attribute_update)
    message_adapter.parse_attribute_update.return_value = gateway_attribute_update
    gateway_attribute_update.set_device_session = AsyncMock()
    
    # Act
    await handler.handle("topic", payload)
    
    # Assert
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)
    message_adapter.parse_attribute_update.assert_called_once_with(deserialized_data)
    device_manager.get_by_name.assert_called_once_with("test_device")
    gateway_attribute_update.set_device_session.assert_called_once_with(device_session)
    event_dispatcher.dispatch.assert_awaited_once_with(attribute_update, device_session=device_session)


@pytest.mark.asyncio
async def test_handle_with_nonexistent_device():
    # Setup
    event_dispatcher = AsyncMock(spec=DirectEventDispatcher)
    message_adapter = MagicMock(spec=GatewayMessageAdapter)
    device_manager = MagicMock(spec=DeviceManager)
    
    # Create a handler
    handler = GatewayAttributeUpdatesHandler(
        event_dispatcher=event_dispatcher,
        message_adapter=message_adapter,
        device_manager=device_manager
    )
    
    # Mock the device session (nonexistent)
    device_manager.get_by_name.return_value = None
    
    # Mock the message adapter
    payload = b'{"device": "nonexistent_device", "data": {"key": "value"}}'
    deserialized_data = {"device": "nonexistent_device", "data": {"key": "value"}}
    message_adapter.deserialize_to_dict.return_value = deserialized_data
    
    # Create a mock attribute update
    attribute_update = AttributeUpdate([AttributeEntry("key", "value")])
    gateway_attribute_update = GatewayAttributeUpdate(device_name="nonexistent_device", attribute_update=attribute_update)
    message_adapter.parse_attribute_update.return_value = gateway_attribute_update
    gateway_attribute_update.set_device_session = AsyncMock()
    
    # Act
    await handler.handle("topic", payload)
    
    # Assert
    message_adapter.deserialize_to_dict.assert_called_once_with(payload)
    message_adapter.parse_attribute_update.assert_called_once_with(deserialized_data)
    device_manager.get_by_name.assert_called_once_with("nonexistent_device")
    # set_device_session should not be called since a device is None
    assert not hasattr(gateway_attribute_update, 'set_device_session') or not gateway_attribute_update.set_device_session.called
    event_dispatcher.dispatch.assert_awaited_once_with(attribute_update, device_session=None)


if __name__ == '__main__':
    pytest.main([__file__])
