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

import pytest

from tb_mqtt_client.entities.gateway.device_disconnect_message import DeviceDisconnectMessage
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


def test_direct_instantiation_not_allowed():
    # Directly calling constructor should raise TypeError
    with pytest.raises(TypeError) as exc_info:
        DeviceDisconnectMessage("DeviceX")
    assert "Direct instantiation of DeviceDisconnectMessage" in str(exc_info.value)


def test_build_and_attributes():
    # Build using correct way
    msg = DeviceDisconnectMessage.build("TestDevice")
    assert isinstance(msg, DeviceDisconnectMessage)
    assert msg.device_name == "TestDevice"
    assert msg.event_type == GatewayEventType.DEVICE_DISCONNECT
    # Check __repr__
    assert repr(msg) == "DeviceDisconnectMessage(device_name=TestDevice)"
    # Check to_payload_format
    assert msg.to_payload_format() == {"device": "TestDevice"}


def test_build_with_empty_device_name():
    # Empty device name should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        DeviceDisconnectMessage.build("")
    assert "Device name must not be empty" in str(exc_info.value)


def test_frozen_slots_and_equality_behavior():
    msg1 = DeviceDisconnectMessage.build("A")
    msg2 = DeviceDisconnectMessage.build("A")
    msg3 = DeviceDisconnectMessage.build("B")

    # Equality check works for dataclass with frozen=True
    assert msg1 == msg2
    assert msg1 != msg3


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
