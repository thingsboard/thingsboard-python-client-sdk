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

from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


class DummyDeviceSession:
    """A simple dummy object to mimic a device session."""
    pass


def test_initialization_and_event_type():
    # Create event with a specific GatewayEventType
    event = BaseGatewayEvent(GatewayEventType.GATEWAY_CONNECT)

    # Ensure the event_type property returns what we set
    assert event.event_type == GatewayEventType.GATEWAY_CONNECT

    # device_session should be None initially
    assert event.device_session is None


def test_set_and_get_device_session():
    event = BaseGatewayEvent(GatewayEventType.GATEWAY_DISCONNECT)

    # Initially None
    assert event.device_session is None

    # Set a device session
    dummy_session = DummyDeviceSession()
    event.set_device_session(dummy_session)

    # Ensure the getter returns what we set
    assert event.device_session is dummy_session


def test_str_calls_repr(monkeypatch):
    event = BaseGatewayEvent(GatewayEventType.GATEWAY_CONNECT)

    # Monkeypatch __repr__ to a known value
    monkeypatch.setattr(event, "__repr__", lambda: "mocked_repr")

    # __str__ should return whatever __repr__ returns
    assert str(event) == "mocked_repr"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
