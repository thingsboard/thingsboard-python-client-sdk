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

from typing import Union

from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.service.gateway.device_session import DeviceSession


class GatewayEvent:
    """
    Base class for all events in the gateway client.
    This class can be extended to create specific event types.
    """

    def __init__(self, event_type: GatewayEventType):
        self.event_type = event_type
        self.__device_session: Union[DeviceSession, None] = None

    def set_device_session(self, device_session: DeviceSession):
        self.__device_session = device_session

    def get_device_session(self) -> Union[DeviceSession, None]:
        return self.__device_session

    def __str__(self) -> str:
        return f"GatewayEvent(type={self.event_type}, device_session={self.__device_session})"

    def to_dict(self) -> dict:
        return {"event_type": self.event_type, "device_session": self.__device_session}
