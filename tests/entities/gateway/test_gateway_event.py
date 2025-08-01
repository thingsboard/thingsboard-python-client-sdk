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

from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_event import GatewayEvent
from tb_mqtt_client.service.gateway.device_session import DeviceSession


def test_initialization_and_event_type():
    # Create an event and check event type
    event = GatewayEvent(GatewayEventType.GATEWAY_CONNECT)
    assert event.event_type == GatewayEventType.GATEWAY_CONNECT

    # Initially __device_session should be None
    assert event.get_device_session() is None


def test_set_and_get_device_session():
    event = GatewayEvent(GatewayEventType.GATEWAY_DISCONNECT)

    device_info = DeviceInfo("dummy_device", "default")
    dummy_session = DeviceSession(device_info)
    event.set_device_session(dummy_session)

    # Ensure get_device_session returns the same object
    assert event.get_device_session() is dummy_session


def test_str_representation_with_and_without_device_session():
    event = GatewayEvent(GatewayEventType.GATEWAY_CONNECT)

    # Without device session
    no_session_str = str(event)
    assert "GatewayEvent(type=" in no_session_str
    assert "device_session=None" in no_session_str

    # With device session
    device_info = DeviceInfo("dummy_device", "default")
    dummy_session = DeviceSession(device_info)
    event.set_device_session(dummy_session)
    session_str = str(event)
    assert "DeviceSession" in session_str


def test_to_dict_with_and_without_device_session():
    event = GatewayEvent(GatewayEventType.GATEWAY_CONNECT)

    # Without device session
    d = event.to_dict()
    assert d["event_type"] == GatewayEventType.GATEWAY_CONNECT
    assert d["device_session"] is None

    # With device session
    device_info = DeviceInfo("dummy_device", "default")
    dummy_session = DeviceSession(device_info)
    event.set_device_session(dummy_session)
    d2 = event.to_dict()
    assert d2["device_session"] is dummy_session


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
