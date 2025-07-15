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
    GATEWAY_CONNECT = "gateway.connect"
    GATEWAY_DISCONNECT = "gateway.disconnect"
    DEVICE_ADD = "gateway.device.add"
    DEVICE_REMOVE = "gateway.device.remove"
    DEVICE_UPDATE = "gateway.device.update"
    DEVICE_SESSION_STATE_CHANGE = "gateway.device.session.state.change"
    DEVICE_RPC_REQUEST_RECEIVE = "gateway.device.rpc.request.receive"
    DEVICE_RPC_RESPONSE_SEND = "gateway.device.rpc.response.send"
    DEVICE_ATTRIBUTE_UPDATE_RECEIVE = "gateway.device.attribute.update.receive"
    DEVICE_REQUESTED_ATTRIBUTE_RESPONSE_RECEIVE = "gateway.device.requested.attribute.response.receive"
    RPC_REQUEST_RECEIVE = "device.rpc.request.receive"
    RPC_RESPONSE_SEND = "device.rpc.response.send"

    def __str__(self) -> str:
        return self.value
