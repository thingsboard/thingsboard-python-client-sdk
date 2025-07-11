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

from abc import ABC, abstractmethod

from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.base_client import BaseClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession


class GatewayClientInterface(BaseClient, ABC):

    @abstractmethod
    async def connect_device(self, device_name: str, device_profile: str) -> DeviceSession: ...

    @abstractmethod
    async def disconnect_device(self,  device_session: DeviceSession): ...

    @abstractmethod
    async def send_device_telemetry(self,  device_session: DeviceSession, telemetry: ...): ...

    @abstractmethod
    async def send_device_attributes(self,  device_session: DeviceSession, attributes: ...): ...

    @abstractmethod
    async def send_device_attributes_request(self, device_session: DeviceSession, attributes: AttributeRequest): ...

    @abstractmethod
    async def send_device_client_side_rpc_request(self, device_session: DeviceSession, rpc_request: RPCRequest): ...

    @abstractmethod
    async def send_device_server_side_rpc_response(self, device_session: DeviceSession, rpc_response: RPCResponse): ...



    @abstractmethod
    def set_device_server_side_rpc_request_callback(self,  device_session: DeviceSession, callback: ...): ...

    @abstractmethod
    def set_device_client_side_rpc_response_callback(self,  device_session: DeviceSession, callback: ...): ...

    @abstractmethod
    def set_device_requested_attributes_callback(self,  device_session: DeviceSession, callback: ...): ...

    @abstractmethod
    def set_device_attributes_update_callback(self,  device_session: DeviceSession, callback: ...): ...
