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
class GatewayRPCRequest:
    request_id: Union[int, str]
    device_name: str
    method: str
    params: Optional[Any] = None

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of GatewayRPCRequest is not allowed. Use 'await GatewayRPCRequest.build(...)'.")

    def __repr__(self):
        return f"RPCRequest(id={self.request_id}, device_name={self.device_name}, method={self.method}, params={self.params})"

    @classmethod
    def _deserialize_from_dict(cls, data: Dict[str, Union[str, Dict[str, Any]]]) -> 'GatewayRPCRequest':
        """
        Constructs an GatewayRPCRequest, should be used only for deserialization request from the platform.
        """
        if "device" not in data:
            raise ValueError("Missing device name in RPC request")
        device_name = data["device"]
        data = data["data"]
        request_id = data["id"]
        if not isinstance(request_id, (int, str)):
            raise ValueError("Missing request id in RPC request")
        if "method" not in data:
            raise ValueError("Missing 'method' in RPC request")

        self = object.__new__(cls)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'method', data["method"])
        object.__setattr__(self, 'params', data.get("params"))
        object.__setattr__(self, 'device', device_name)
        return self
