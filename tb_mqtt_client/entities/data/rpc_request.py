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

from tb_mqtt_client.common.request_id_generator import RPCRequestIdProducer
from tb_mqtt_client.constants.json_typing import validate_json_compatibility, JSONCompatibleType


@dataclass(slots=True, frozen=True)
class RPCRequest:
    request_id: Union[int, str]
    method: str
    params: Optional[Any] = None

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of RPCRequest is not allowed. Use 'await RPCRequest.build(...)'.")

    def __repr__(self):
        return f"RPCRequest(id={self.request_id}, method={self.method}, params={self.params})"

    @classmethod
    def _deserialize_from_dict(cls, request_id: int, data: Dict[str, Any]) -> 'RPCRequest':
        """
        Constructs an RPCRequest, should be used only for deserialization request from the platform.
        """
        if not isinstance(request_id, (int, str)):
            raise ValueError("Missing request id in RPC request")
        if "method" not in data:
            raise ValueError("Missing 'method' in RPC request")

        self = object.__new__(cls)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'method', data["method"])
        object.__setattr__(self, 'params', data.get("params"))
        return self

    @classmethod
    async def build(cls, method: str, params: Optional[JSONCompatibleType] = None) -> 'RPCRequest':
        """
        Constructs an RPCRequest with a unique request ID,
        using the RPCRequestIdProducer to ensure thread-safe ID generation.
        """
        if not isinstance(method, str):
            raise ValueError("Method must be a string")
        validate_json_compatibility(params)
        request_id = await RPCRequestIdProducer.get_next()
        self = object.__new__(cls)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'method', method)
        object.__setattr__(self, 'params', params)
        return self

    def to_payload_format(self) -> Dict[str, Any]:
        """
        Serializes the RPC request for publishing.
        Converts the request to a dictionary format suitable for MQTT payload.
        """
        data = {
            "id": self.request_id,
            "method": self.method
        }
        if self.params is not None:
            data["params"] = self.params
        return data
