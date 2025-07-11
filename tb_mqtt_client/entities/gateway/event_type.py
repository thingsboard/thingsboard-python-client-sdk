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

from enum import Enum

class GatewayEventType(Enum):
    """
    Enum representing different types of gateway events.
    Each event type corresponds to a specific action or state change in the gateway.
    """
    DEVICE_ADDED = "DEVICE_ADDED"
    DEVICE_REMOVED = "DEVICE_REMOVED"
    DEVICE_UPDATED = "DEVICE_UPDATED"
    DEVICE_SESSION_STATE_CHANGED = "DEVICE_SESSION_STATE_CHANGED"
    DEVICE_RPC_REQUEST_RECEIVED = "DEVICE_RPC_REQUEST_RECEIVED"
    DEVICE_RPC_RESPONSE_SENT = "DEVICE_RPC_RESPONSE_SENT"
    DEVICE_ATTRIBUTE_UPDATE_RECEIVED = "DEVICE_ATTRIBUTE_UPDATE_RECEIVED"
    DEVICE_REQUESTED_ATTRIBUTE_RESPONSE_RECEIVED = "DEVICE_REQUESTED_ATTRIBUTE_RESPONSE_RECEIVED"
    RPC_REQUEST_RECEIVED = "RPC_REQUEST_RECEIVED"
    RPC_RESPONSE_SENT = "RPC_RESPONSE_SENT"
    GATEWAY_CONNECTED = "GATEWAY_CONNECTED"
    GATEWAY_DISCONNECTED = "GATEWAY_DISCONNECTED"

    def __str__(self) -> str:
        return self.value
