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

from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


def test_direct_instantiation_not_allowed():
    # Direct instantiation should raise a TypeError
    with pytest.raises(TypeError) as exc_info:
        DeviceConnectMessage("device1")
    assert "Direct instantiation" in str(exc_info.value)


def test_build_with_empty_device_name_raises():
    # Empty device name should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        DeviceConnectMessage.build("")
    assert "must not be empty" in str(exc_info.value)


def test_build_and_repr_and_payload():
    msg = DeviceConnectMessage.build("MyDevice", "MyProfile")

    # Check attributes
    assert msg.device_name == "MyDevice"
    assert msg.device_profile == "MyProfile"
    assert msg.event_type == GatewayEventType.DEVICE_CONNECT

    # __repr__ format check
    repr_str = repr(msg)
    assert "DeviceConnectMessage" in repr_str
    assert "MyDevice" in repr_str
    assert "MyProfile" in repr_str

    # Payload format check
    payload = msg.to_payload_format()
    assert payload == {"device": "MyDevice", "type": "MyProfile"}


def test_build_with_default_profile():
    msg = DeviceConnectMessage.build("DefaultDevice")
    assert msg.device_profile == "default"
    assert msg.to_payload_format() == {"device": "DefaultDevice", "type": "default"}


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
