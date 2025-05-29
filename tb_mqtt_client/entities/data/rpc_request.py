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
class RPCRequest:
    request_id: Union[int, str]
    method: str
    params: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "id": self.request_id,
            "method": self.method
        }
        if self.params is not None:
            result["params"] = self.params
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RPCRequest':
        if "id" not in data:
            raise ValueError("Missing 'id' in RPC request")
        if "method" not in data:
            raise ValueError("Missing 'method' in RPC request")

        return cls(
            request_id=data["id"],
            method=data["method"],
            params=data.get("params")
        )
