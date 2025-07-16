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
from traceback import format_exception
from typing import Union, Optional, Dict, Any

from tb_mqtt_client.constants.json_typing import validate_json_compatibility, JSONCompatibleType
from tb_mqtt_client.entities.data.rpc_response import RPCStatus, RPCResponse
from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


@dataclass(slots=True, frozen=True)
class GatewayRPCResponse(RPCResponse, BaseGatewayEvent):
    """
    Represents a response to the RPC call.

    Attributes:
        device_name: Name of the device for which the RPC response is intended.
        request_id: Unique identifier of the request being responded to.
        result: Optional response payload (Any type allowed).
        error: Optional error information if the RPC failed.
    """
    device_name: str = None
    event_type: GatewayEventType = GatewayEventType.DEVICE_RPC_RESPONSE

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of GatewayRPCResponse is not allowed. Use GatewayRPCResponse.build(device_name, request_id, result, error).")

    def __repr__(self) -> str:
        return f"GatewayRPCResponse(device_name={self.device_name}, request_id={self.request_id}, result={self.result}, error={self.error})"

    @classmethod
    def build(cls, # noqa
              device_name: str,
              request_id: int,
              result: Optional[Any] = None,
              error: Optional[Union[str, Dict[str, JSONCompatibleType], BaseException]] = None) -> 'GatewayRPCResponse':
        """
        Constructs an GatewayRPCResponse explicitly.
        """
        if not isinstance(device_name, str) or not device_name:
            raise ValueError("Device name must be a non-empty string")
        self = object.__new__(cls)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'device_name', device_name)

        if error is not None:
            if not isinstance(error, (str, dict, BaseException)):
                raise ValueError("Error must be a string, dictionary, or an exception instance")

            object.__setattr__(self, 'status', RPCStatus.ERROR)

            if isinstance(error, BaseException):
                try:
                    raise error
                except BaseException as e:
                    error = {
                        "message": str(e),
                        "type": type(e).__name__,
                        "details": ''.join(format_exception(type(e), e, e.__traceback__))
                    }

            validate_json_compatibility(error)
            object.__setattr__(self, 'error', error)

        else:
            object.__setattr__(self, 'status', RPCStatus.SUCCESS)
            object.__setattr__(self, 'error', None)
            validate_json_compatibility(result)

        object.__setattr__(self, 'result', result)
        object.__setattr__(self, 'event_type', GatewayEventType.DEVICE_RPC_RESPONSE)
        return self

    def to_payload_format(self) -> Dict[str, Any]:
        """Serializes the RPC response for publishing."""
        data = {"device": self.device_name, "id": self.request_id, "data": {}}
        if self.result is not None:
            data["data"]["result"] = self.result
        if self.error is not None:
            data["data"]["error"] = self.error
        return data
