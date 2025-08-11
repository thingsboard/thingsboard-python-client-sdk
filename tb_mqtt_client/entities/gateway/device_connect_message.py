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

from dataclasses import dataclass
from typing import Dict

from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


@dataclass(slots=True, frozen=True)
class DeviceConnectMessage(BaseGatewayEvent):
    """
    Represents a device connection message in the ThingsBoard Gateway MQTT client.
    This class is used to encapsulate the details of a device connection message.
    """
    device_name: str
    device_profile: str = 'default'
    event_type: GatewayEventType = GatewayEventType.DEVICE_CONNECT

    def __new__(cls, *args, **kwargs):
        raise TypeError(
            "Direct instantiation of DeviceConnectMessage is not allowed. Use 'await DeviceConnectMessage.build(...)'.")

    def __repr__(self):
        return f"DeviceConnectMessage(device_name={self.device_name}, device_profile={self.device_profile})"

    @classmethod
    def build(cls, device_name: str, device_profile: str = 'default') -> 'DeviceConnectMessage':
        """
        Build a new DeviceConnectMessage with the specified device name and profile.
        """
        if not device_name:
            raise ValueError("Device name must not be empty.")
        self = object.__new__(cls)
        object.__setattr__(self, 'device_name', device_name)
        object.__setattr__(self, 'device_profile', device_profile)
        object.__setattr__(self, 'event_type', GatewayEventType.DEVICE_CONNECT)
        return self

    def to_payload_format(self) -> Dict[str, str]:
        """
        Convert the device connection message into the expected MQTT payload format.
        """
        return {
            "device": self.device_name,
            "type": self.device_profile
        }
