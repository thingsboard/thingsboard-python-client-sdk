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

from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


class BaseGatewayEvent:
    def __init__(self, event_type: GatewayEventType):
        self.__event_type = event_type
        self.__device_session = None

    @property
    def event_type(self) -> GatewayEventType:
        return self.__event_type

    def set_device_session(self, device_session):
        self.__device_session = device_session

    @property
    def device_session(self):
        return self.__device_session

    def __str__(self) -> str:
        return self.__repr__()
