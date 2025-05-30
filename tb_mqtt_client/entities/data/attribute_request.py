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
from typing import Optional, List, Dict
from tb_mqtt_client.common.request_id_generator import AttributeRequestIdProducer
from tb_mqtt_client.constants.json_typing import validate_json_compatibility


@dataclass(slots=True, frozen=True)
class AttributeRequest:
    """
    Represents a request for device attributes, with optional client and shared attribute keys.
    Automatically assigns a unique request ID via the build() method.
    """
    request_id: int
    shared_keys: Optional[List[str]] = None
    client_keys: Optional[List[str]] = None

    def __new__(self, *args, **kwargs):
        raise TypeError("Direct instantiation of AttributeRequest is not allowed. Use 'await AttributeRequest.build(...)'.")

    def __repr__(self) -> str:
        return f"AttributeRequest(id={self.request_id}, shared_keys={self.shared_keys}, client_keys={self.client_keys})"

    @classmethod
    async def build(cls, shared_keys: Optional[List[str]] = None, client_keys: Optional[List[str]] = None) -> 'AttributeRequest':
        """
        Build a new AttributeRequest with a unique request ID, using the global ID generator.
        """
        validate_json_compatibility(shared_keys)
        validate_json_compatibility(client_keys)
        request_id = await AttributeRequestIdProducer.get_next()
        self = object.__new__(cls)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'shared_keys', shared_keys)
        object.__setattr__(self, 'client_keys', client_keys)
        return self

    def to_payload_format(self) -> Dict[str, str]:
        """
        Convert the attribute request into the expected MQTT payload format.
        """
        payload = {}
        if self.shared_keys is not None:
            payload["sharedKeys"] = ','.join(self.shared_keys)
        if self.client_keys is not None:
            payload["clientKeys"] = ','.join(self.client_keys)
        return payload
