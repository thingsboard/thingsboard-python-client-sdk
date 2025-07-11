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

from time import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Optional, Dict, Any

from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.device_session_state import DeviceSessionState


@dataclass
class DeviceSession:
    device_info: DeviceInfo
    _state_change_callback: Optional[Callable[['DeviceSession'], None]] = None
    connected_at: float = field(default_factory=lambda: int(time() * 1000))
    last_seen_at: float = field(default_factory=lambda: int(time() * 1000))
    claimed: bool = False
    provisioned: bool = False
    state: DeviceSessionState = DeviceSessionState.CONNECTED

    attribute_update_callback: Optional[Callable[[dict], Awaitable[None]]] = None
    attribute_response_callback: Optional[Callable[[dict], Awaitable[None]]] = None
    rpc_request_callback: Optional[Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None
    rpc_response_callback: Optional[Callable[[dict], Awaitable[None]]] = None

    def update_state(self, new_state: DeviceSessionState):
        self.state = new_state
        if self._state_change_callback:
            self._state_change_callback(self)

    def update_last_seen(self):
        self.last_seen_at = int(time() * 1000)

    def set_attribute_update_callback(self, cb: Callable[[dict], Awaitable[None]]):
        self.attribute_update_callback = cb

    def set_attribute_response_callback(self, cb: Callable[[dict], Awaitable[None]]):
        self.attribute_response_callback = cb

    def set_rpc_request_callback(self, cb: Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        self.rpc_request_callback = cb

    def set_rpc_response_callback(self, cb: Callable[[dict], Awaitable[None]]):
        self.rpc_response_callback = cb
