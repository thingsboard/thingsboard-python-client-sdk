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
from typing import Optional, List, Dict, Union
from tb_mqtt_client.common.request_id_generator import AttributeRequestIdProducer
from tb_mqtt_client.constants.json_typing import validate_json_compatibility
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent


@dataclass(slots=True, frozen=True)
class GatewayAttributeRequest(BaseGatewayEvent):
    """
    Represents a request for device attributes, with optional client and shared attribute keys.
    Automatically assigns a unique request ID via the build() method.
    """
    request_id: int
    device_name: str
    shared_keys: Optional[List[str]] = None
    client_keys: Optional[List[str]] = None
    event_type = GatewayEventType.DEVICE_REQUESTED_ATTRIBUTE_RESPONSE_RECEIVE

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of GatewayAttributeRequest is not allowed. Use 'await GatewayAttributeRequest.build(...)'.")

    def __repr__(self) -> str:
        return f"GatewayAttributeRequest(device_name={self.device_name}, id={self.request_id}, shared_keys={self.shared_keys}, client_keys={self.client_keys})"

    @classmethod
    async def build(cls, device_name: str, shared_keys: Optional[List[str]] = None, client_keys: Optional[List[str]] = None) -> 'GatewayAttributeRequest':
        """
        Build a new GatewayAttributeRequest with a unique request ID, using the global ID generator.
        """
        validate_json_compatibility(shared_keys)
        validate_json_compatibility(client_keys)
        request_id = await AttributeRequestIdProducer.get_next()
        self = object.__new__(cls)
        object.__setattr__(self, 'device_name', device_name)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'shared_keys', shared_keys)
        object.__setattr__(self, 'client_keys', client_keys)
        return self

    def to_payload_format(self) -> Dict[str, Union[str, bool]]:
        """
        Convert the attribute request into the expected MQTT payload format.
        """
        payload = {"device": self.device_name, "id": str(self.request_id)}
        request_key = 'key' if len(self.client_keys) == 1 or len(self.shared_keys) == 1 else 'keys'
        if self.client_keys:
            payload['client'] = True
            payload[request_key] = ','.join(self.client_keys)
        elif self.shared_keys:
            # TODO: In current realisation on server it is not possible to request values for the both scopes simultaneously, recommended to improve the platform API
            payload['client'] = False
            payload[request_key] = ','.join(self.shared_keys)
        return payload
