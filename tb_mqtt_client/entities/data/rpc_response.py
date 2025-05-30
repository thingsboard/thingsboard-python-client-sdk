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
from typing import Union, Optional, Dict, Any

from tb_mqtt_client.constants.json_typing import validate_json_compatibility


@dataclass(slots=True, frozen=True)
class RPCResponse:
    """
    Represents a response to the RPC call.

    Attributes:
        request_id: Unique identifier of the request being responded to.
        result: Optional response payload (Any type allowed).
        error: Optional error information if the RPC failed.
    """
    request_id: Union[int, str]
    result: Optional[Any] = None
    error: Optional[Union[str, Dict[str, Any]]] = None

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of RPCResponse is not allowed. Use RPCResponse.build(request_id, result, error).")

    def __repr__(self) -> str:
        return f"RPCResponse(request_id={self.request_id}, result={self.result}, error={self.error})"

    @classmethod
    def build(cls, request_id: Union[int, str], result: Optional[Any] = None, error: Optional[Union[str, Dict[str, Any]]] = None) -> 'RPCResponse':
        """
        Constructs an RPCResponse explicitly.
        """
        self = object.__new__(cls)
        object.__setattr__(self, 'request_id', request_id)
        validate_json_compatibility(result)
        object.__setattr__(self, 'result', result)
        validate_json_compatibility(error)
        object.__setattr__(self, 'error', error)
        return self

    def to_payload_format(self) -> Dict[str, Any]:
        """Serializes the RPC response for publishing."""
        data = {}
        if self.result is not None:
            data["result"] = self.result
        if self.error is not None:
            data["error"] = self.error
        return data
