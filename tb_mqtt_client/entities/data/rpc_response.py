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


@dataclass(slots=True, frozen=True)
class RPCResponse:
    """
    Represents a response to a device-side RPC call.

    Attributes:
        request_id: Unique identifier of the request being responded to.
        result: Optional response payload (Any type allowed).
        error: Optional error information if the RPC failed.
    """
    request_id: Union[int, str]
    result: Optional[Any] = None
    error: Optional[Union[str, Dict[str, Any]]] = None

    def to_payload_format(self) -> Dict[str, Any]:
        """Serializes the RPC response for publishing."""
        data = {}
        if self.result is not None:
            data["result"] = self.result
        if self.error is not None:
            data["error"] = self.error
        return data
