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

from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.device_session import DeviceSession


def test_register_new_device():
    # Setup
    manager = DeviceManager()
    
    # Act
    session = manager.register("test_device", "default")
    
    # Assert
    assert session is not None
    assert session.device_info.device_name == "test_device"
    assert session.device_info.device_profile == "default"
    assert session.device_info.device_id in manager._sessions_by_id
    assert "test_device" in manager._ids_by_device_name
    assert session.device_info.original_name in manager._ids_by_original_name


def test_register_existing_device():
    # Setup
    manager = DeviceManager()
    first_session = manager.register("test_device", "default")
    
    # Act
    second_session = manager.register("test_device", "custom")
    
    # Assert
    assert first_session is second_session
    assert second_session.device_info.device_profile == "default"  # Profile should not change


def test_unregister_device():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    device_id = session.device_info.device_id
    
    # Act
    manager.unregister(device_id)
    
    # Assert
    assert device_id not in manager._sessions_by_id
    assert "test_device" not in manager._ids_by_device_name
    assert session.device_info.original_name not in manager._ids_by_original_name


def test_unregister_nonexistent_device():
    # Setup
    manager = DeviceManager()
    from uuid import uuid4
    nonexistent_id = uuid4()
    
    # Act & Assert - should not raise an exception
    manager.unregister(nonexistent_id)


def test_get_by_id():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    device_id = session.device_info.device_id
    
    # Act
    retrieved_session = manager.get_by_id(device_id)
    
    # Assert
    assert retrieved_session is session


def test_get_by_id_nonexistent():
    # Setup
    manager = DeviceManager()
    from uuid import uuid4
    nonexistent_id = uuid4()
    
    # Act
    retrieved_session = manager.get_by_id(nonexistent_id)
    
    # Assert
    assert retrieved_session is None


def test_get_by_name():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    
    # Act
    retrieved_session = manager.get_by_name("test_device")
    
    # Assert
    assert retrieved_session is session


def test_get_by_original_name():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    original_name = session.device_info.original_name
    
    # Rename the device
    manager.rename_device("test_device", "new_name")
    
    # Act
    retrieved_session = manager.get_by_name(original_name)
    
    # Assert
    assert retrieved_session is session


def test_get_by_name_nonexistent():
    # Setup
    manager = DeviceManager()
    
    # Act
    retrieved_session = manager.get_by_name("nonexistent_device")
    
    # Assert
    assert retrieved_session is None


def test_is_connected():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    device_id = session.device_info.device_id
    
    # Act
    is_connected = manager.is_connected(device_id)
    
    # Assert
    assert is_connected is True


def test_is_connected_nonexistent():
    # Setup
    manager = DeviceManager()
    from uuid import uuid4
    nonexistent_id = uuid4()
    
    # Act
    is_connected = manager.is_connected(nonexistent_id)
    
    # Assert
    assert is_connected is False


def test_all():
    # Setup
    manager = DeviceManager()
    session1 = manager.register("device1", "default")
    session2 = manager.register("device2", "default")
    
    # Act
    all_sessions = list(manager.all())
    
    # Assert
    assert len(all_sessions) == 2
    assert session1 in all_sessions
    assert session2 in all_sessions


def test_rename_device():
    # Setup
    manager = DeviceManager()
    session = manager.register("old_name", "default")
    device_id = session.device_info.device_id
    
    # Act
    manager.rename_device("old_name", "new_name")
    
    # Assert
    assert "old_name" not in manager._ids_by_device_name
    assert "new_name" in manager._ids_by_device_name
    assert manager._ids_by_device_name["new_name"] == device_id
    assert session.device_info.device_name == "new_name"
    assert session.device_info.original_name in manager._ids_by_original_name


def test_rename_nonexistent_device():
    # Setup
    manager = DeviceManager()
    
    # Act - should not raise an exception
    manager.rename_device("nonexistent_device", "new_name")
    
    # Assert
    assert "nonexistent_device" not in manager._ids_by_device_name
    assert "new_name" not in manager._ids_by_device_name


def test_set_attribute_update_callback():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    device_id = session.device_info.device_id
    callback = MagicMock()
    
    # Mock the session's set_attribute_update_callback method
    session.set_attribute_update_callback = MagicMock()
    
    # Act
    manager.set_attribute_update_callback(device_id, callback)
    
    # Assert
    session.set_attribute_update_callback.assert_called_once_with(callback)


def test_set_attribute_update_callback_nonexistent():
    # Setup
    manager = DeviceManager()
    from uuid import uuid4
    nonexistent_id = uuid4()
    callback = MagicMock()
    
    # Act - should not raise an exception
    manager.set_attribute_update_callback(nonexistent_id, callback)


def test_set_attribute_response_callback():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    device_id = session.device_info.device_id
    callback = MagicMock()
    
    # Mock the session's set_attribute_response_callback method
    session.set_attribute_response_callback = MagicMock()
    
    # Act
    manager.set_attribute_response_callback(device_id, callback)
    
    # Assert
    session.set_attribute_response_callback.assert_called_once_with(callback)


def test_set_attribute_response_callback_nonexistent():
    # Setup
    manager = DeviceManager()
    from uuid import uuid4
    nonexistent_id = uuid4()
    callback = MagicMock()
    
    # Act - should not raise an exception
    manager.set_attribute_response_callback(nonexistent_id, callback)


def test_set_rpc_request_callback():
    # Setup
    manager = DeviceManager()
    session = manager.register("test_device", "default")
    device_id = session.device_info.device_id
    callback = MagicMock()
    
    # Mock the session's set_rpc_request_callback method
    session.set_rpc_request_callback = MagicMock()
    
    # Act
    manager.set_rpc_request_callback(device_id, callback)
    
    # Assert
    session.set_rpc_request_callback.assert_called_once_with(callback)


def test_set_rpc_request_callback_nonexistent():
    # Setup
    manager = DeviceManager()
    from uuid import uuid4
    nonexistent_id = uuid4()
    callback = MagicMock()
    
    # Act - should not raise an exception
    manager.set_rpc_request_callback(nonexistent_id, callback)


def test_state_change_callback():
    # Setup
    manager = DeviceManager()
    
    # Create a device session with a mocked state
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info, manager._DeviceManager__state_change_callback)
    
    # Mock the session's state
    session.state = MagicMock()
    session.state.is_connected = MagicMock(return_value=True)
    
    # Act
    manager._DeviceManager__state_change_callback(session)
    
    # Assert
    assert session in manager.connected_devices


def test_state_change_callback_disconnect():
    # Setup
    manager = DeviceManager()
    
    # Create a device session with a mocked state
    device_info = DeviceInfo("test_device", "default")
    session = DeviceSession(device_info, manager._DeviceManager__state_change_callback)
    
    # Add the session to connected devices
    manager._DeviceManager__connected_devices.add(session)
    
    # Mock the session's state
    session.state = MagicMock()
    session.state.is_connected = MagicMock(return_value=False)
    
    # Act
    manager._DeviceManager__state_change_callback(session)
    
    # Assert
    assert session not in manager.connected_devices


def test_connected_devices_property():
    # Setup
    manager = DeviceManager()
    session1 = manager.register("device1", "default")
    session2 = manager.register("device2", "default")
    
    # Add sessions to connected devices
    manager._DeviceManager__connected_devices.add(session1)
    manager._DeviceManager__connected_devices.add(session2)
    
    # Act
    connected = manager.connected_devices
    
    # Assert
    assert len(connected) == 2
    assert session1 in connected
    assert session2 in connected


def test_all_devices_property():
    # Setup
    manager = DeviceManager()
    session1 = manager.register("device1", "default")
    session2 = manager.register("device2", "default")
    
    # Act
    all_devices = manager.all_devices
    
    # Assert
    assert len(all_devices) == 2
    assert session1.device_info.device_id in all_devices
    assert session2.device_info.device_id in all_devices
    assert all_devices[session1.device_info.device_id] is session1
    assert all_devices[session2.device_info.device_id] is session2


def test_repr():
    # Setup
    manager = DeviceManager()
    manager.register("device1", "default")
    manager.register("device2", "default")
    
    # Act
    repr_string = repr(manager)
    
    # Assert
    assert "DeviceManager" in repr_string
    assert "device1" in repr_string
    assert "device2" in repr_string


if __name__ == '__main__':
    pytest.main([__file__])