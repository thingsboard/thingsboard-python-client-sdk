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
import asyncio
from time import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Optional, Union

from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent

from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.device_session_state import DeviceSessionState
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse


@dataclass
class DeviceSession:
    device_info: DeviceInfo
    _state_change_callback: Optional[Callable[['DeviceSession'], None]] = None
    connected_at: float = field(default_factory=lambda: int(time() * 1000))
    last_seen_at: float = field(default_factory=lambda: int(time() * 1000))
    claimed: bool = False
    provisioned: bool = False
    state: DeviceSessionState = DeviceSessionState.CONNECTED

    attribute_update_callback: Optional[Callable[['DeviceSession','AttributeUpdate'], Union[Awaitable[None], None]]] = None
    attribute_response_callback: Optional[Callable[['DeviceSession','RequestedAttributeResponse'], Union[Awaitable[None], None]]] = None
    rpc_request_callback: Optional[Callable[['DeviceSession','GatewayRPCRequest'], Union[Awaitable[Union['GatewayRPCResponse', None]], None]]] = None

    def update_state(self, new_state: DeviceSessionState):
        self.state = new_state
        if self._state_change_callback:
            self._state_change_callback(self)

    def update_last_seen(self):
        self.last_seen_at = int(time() * 1000)

    def set_attribute_update_callback(self, cb: Callable[['DeviceSession','AttributeUpdate'], Union[Awaitable[None], None]]):
        self.attribute_update_callback = cb

    def set_attribute_response_callback(self, cb: Callable[['DeviceSession','RequestedAttributeResponse'], Union[Awaitable[None], None]]):
        self.attribute_response_callback = cb

    def set_rpc_request_callback(self, cb: Callable[['DeviceSession','GatewayRPCRequest'], Union[Awaitable[Union['GatewayRPCResponse', None]], None]]):
        self.rpc_request_callback = cb

    async def handle_event_to_device(self, event: BaseGatewayEvent) -> Optional[Awaitable[Union['GatewayRPCResponse', None]]]:
        cb = None
        if GatewayEventType.DEVICE_ATTRIBUTE_UPDATE == event.event_type \
                and isinstance(event, AttributeUpdate):
            if self.attribute_update_callback:
                cb = self.attribute_update_callback
        elif GatewayEventType.DEVICE_REQUESTED_ATTRIBUTE_RESPONSE == event.event_type \
                and isinstance(event, RequestedAttributeResponse):
            if self.attribute_response_callback:
                cb = self.attribute_response_callback
        elif GatewayEventType.DEVICE_RPC_REQUEST == event.event_type \
                and isinstance(event, GatewayRPCRequest):
            if self.rpc_request_callback:
                cb = self.rpc_request_callback

        if cb is None:
            return None

        if asyncio.iscoroutinefunction(cb):
            return await cb(self, event)
        else:
            return cb(self, event)
