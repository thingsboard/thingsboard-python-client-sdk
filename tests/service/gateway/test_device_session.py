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


from unittest.mock import MagicMock

import pytest

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.device_session_state import DeviceSessionState
from tb_mqtt_client.entities.gateway.gateway_attribute_update import GatewayAttributeUpdate
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.service.gateway.device_session import DeviceSession


def test_init():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    state_change_callback = MagicMock()
    
    # Act
    session = DeviceSession(device_info, state_change_callback)
    
    # Assert
    assert session.device_info == device_info
    assert session._state_change_callback == state_change_callback
    assert session.state.is_connected()
    assert session.connected_at > 0
    assert session.last_seen_at > 0
    assert session.claimed is False
    assert session.provisioned is False
    assert session.attribute_update_callback is None
    assert session.attribute_response_callback is None
    assert session.rpc_request_callback is None


def test_update_state():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    state_change_callback = MagicMock()
    session = DeviceSession(device_info, state_change_callback)
    
    # Act
    session.update_state(DeviceSessionState.DISCONNECTED)
    
    # Assert
    assert session.state == DeviceSessionState.DISCONNECTED
    state_change_callback.assert_called_once_with(session)


def test_update_state_no_callback():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)  # No callback provided
    
    # Act - should not raise an exception
    session.update_state(DeviceSessionState.DISCONNECTED)
    
    # Assert
    assert session.state == DeviceSessionState.DISCONNECTED


def test_update_last_seen():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    old_last_seen = session.last_seen_at
    
    # Wait a bit to ensure the timestamp changes
    import time
    time.sleep(0.001)
    
    # Act
    session.update_last_seen()
    
    # Assert
    assert session.last_seen_at > old_last_seen


def test_set_attribute_update_callback():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    callback = MagicMock()
    
    # Act
    session.set_attribute_update_callback(callback)
    
    # Assert
    assert session.attribute_update_callback == callback


def test_set_attribute_response_callback():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    callback = MagicMock()
    
    # Act
    session.set_attribute_response_callback(callback)
    
    # Assert
    assert session.attribute_response_callback == callback


def test_set_rpc_request_callback():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    callback = MagicMock()
    
    # Act
    session.set_rpc_request_callback(callback)
    
    # Assert
    assert session.rpc_request_callback == callback


@pytest.mark.asyncio
async def test_handle_event_to_device_attribute_update():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    callback = MagicMock()
    session.set_attribute_update_callback(callback)
    
    # Create an attribute update event
    attribute_entry = AttributeEntry(key="key", value="value")
    attribute_update = AttributeUpdate([attribute_entry])

    gateway_attribute_update = GatewayAttributeUpdate(session.device_info.device_name, attribute_update=attribute_update)
    
    # Act
    result = await session.handle_event_to_device(gateway_attribute_update.attribute_update)
    
    # Assert
    callback.assert_called_once_with(session, attribute_update)
    assert result == callback.return_value


@pytest.mark.asyncio
async def test_handle_event_to_device_attribute_response():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    callback = MagicMock()
    session.set_attribute_response_callback(callback)
    
    # Create an attribute response event
    gateway_requested_attribute_response = GatewayRequestedAttributeResponse(request_id=1, device_name="test_device", shared=[AttributeEntry(key="shared_key", value="shared_value")], client=[])
    
    # Act
    result = await session.handle_event_to_device(gateway_requested_attribute_response)
    
    # Assert
    callback.assert_called_once_with(session, gateway_requested_attribute_response)
    assert result == callback.return_value


@pytest.mark.asyncio
async def test_handle_event_to_device_rpc_request():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    callback = MagicMock()
    session.set_rpc_request_callback(callback)
    
    # Create an RPC request event
    request_dict = {
        "device": "test_device",
        "data": {
            "id": 1,
            "method": "test",
            "params": {"param": "value"}
        }
    }
    rpc_request = GatewayRPCRequest._deserialize_from_dict(request_dict)
    
    # Act
    result = await session.handle_event_to_device(rpc_request)
    
    # Assert
    callback.assert_called_once_with(session, rpc_request)
    assert result == callback.return_value


@pytest.mark.asyncio
async def test_handle_event_to_device_no_callback():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    
    # Create an attribute update event
    attribute_update = AttributeUpdate([AttributeEntry(key="shared_key", value="shared_value")])
    gateway_attribute_update = GatewayAttributeUpdate(session.device_info.device_name, attribute_update=attribute_update)
    
    # Act
    result = await session.handle_event_to_device(gateway_attribute_update.attribute_update)
    
    # Assert
    assert result is None


@pytest.mark.asyncio
async def test_handle_event_to_device_async_callback():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)

    rpc_request = {
        "device": "test_device",
        "data": {
            "id": 1,
            "method": "test",
            "params": {"param": "value"}
        }
    }
    
    # Create an async callback
    async def async_callback(session, event: GatewayRPCRequest):
        return GatewayRPCResponse.build(device_name=event.device_name, request_id=event.request_id, result="success")
    
    session.set_rpc_request_callback(async_callback)
    
    # Create an RPC request event
    rpc_request = GatewayRPCRequest._deserialize_from_dict(rpc_request)
    
    # Act
    result = await session.handle_event_to_device(rpc_request)
    
    # Assert
    assert isinstance(result, GatewayRPCResponse)
    assert result.device_name == "test_device"
    assert result.request_id == 1
    assert result.result == "success"


@pytest.mark.asyncio
async def test_handle_event_to_device_unsupported_event():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    
    # Create a mock event with an unsupported event type
    mock_event = MagicMock()
    mock_event.event_type = "UNSUPPORTED_EVENT_TYPE"
    
    # Act
    result = await session.handle_event_to_device(mock_event)
    
    # Assert
    assert result is None


def test_equality():
    # Setup
    device_info1 = DeviceInfo("test_device", "default")
    device_info2 = DeviceInfo("test_device", "default")  # Same device name, but different UUID
    device_info3 = DeviceInfo("other_device", "default")
    
    session1 = DeviceSession(device_info1)
    session2 = DeviceSession(device_info2)
    session3 = DeviceSession(device_info3)
    
    # Act & Assert
    assert session1 != session2  # Different UUIDs
    assert session1 != session3
    assert session2 != session3
    
    # Test equality with the same device_info
    session4 = DeviceSession(device_info1)
    assert session1 == session4  # Same device_info


def test_hash():
    # Setup
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info)
    
    # Act
    hash_value = hash(session)
    
    # Assert
    assert hash_value == hash(device_info)
    
    # Test that sessions with the same device_info have the same hash
    session2 = DeviceSession(device_info)
    assert hash(session) == hash(session2)


if __name__ == '__main__':
    pytest.main([__file__])
