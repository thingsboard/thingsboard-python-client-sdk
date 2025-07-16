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
class DeviceDisconnectMessage(BaseGatewayEvent):
    """
    Represents a device disconnection message in the ThingsBoard Gateway MQTT client.
    This class is used to encapsulate the details of a device connection message.
    """
    device_name: str
    event_type: GatewayEventType = GatewayEventType.DEVICE_DISCONNECT

    def __new__(self, *args, **kwargs):
        raise TypeError(
            "Direct instantiation of DeviceDisconnectMessage is not allowed. Use 'DeviceDisconnectMessage.build(...)'.")

    def __repr__(self):
        return f"DeviceDisconnectMessage(device_name={self.device_name})"

    @classmethod
    def build(cls, device_name: str) -> 'DeviceDisconnectMessage':
        """
        Build a new DeviceDisconnectMessage with the specified device name and profile.
        """
        if not device_name:
            raise ValueError("Device name must not be empty.")
        self = object.__new__(cls)
        object.__setattr__(self, 'device_name', device_name)
        object.__setattr__(self, 'event_type', GatewayEventType.DEVICE_DISCONNECT)
        return self

    def to_payload_format(self) -> Dict[str, str]:
        """
        Convert the device connection message into the expected MQTT payload format.
        """
        return {
            "device": self.device_name
        }
