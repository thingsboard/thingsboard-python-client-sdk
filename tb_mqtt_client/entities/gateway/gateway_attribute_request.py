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
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.service.gateway.device_session import DeviceSession


@dataclass(slots=True, frozen=True)
class GatewayAttributeRequest(AttributeRequest):
    """
    Represents a request for device attributes, with optional client and shared attribute keys.
    Automatically assigns a unique request ID via the build() method.
    """
    device_session: DeviceSession = None  # type: ignore[assignment]

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of GatewayAttributeRequest is not allowed. Use 'await GatewayAttributeRequest.build(...)'.")

    def __repr__(self) -> str:
        return f"GatewayAttributeRequest(device_session={self.device_session}, id={self.request_id}, shared_keys={self.shared_keys}, client_keys={self.client_keys})"

    @classmethod
    async def build(cls, device_session: DeviceSession, shared_keys: Optional[List[str]] = None, client_keys: Optional[List[str]] = None) -> 'GatewayAttributeRequest':  # noqa
        """
        Build a new GatewayAttributeRequest with a unique request ID, using the global ID generator.
        """
        validate_json_compatibility(shared_keys)
        validate_json_compatibility(client_keys)
        request_id = await AttributeRequestIdProducer.get_next()
        self = object.__new__(cls)
        object.__setattr__(self, 'device_session', device_session)
        object.__setattr__(self, 'request_id', request_id)
        object.__setattr__(self, 'shared_keys', shared_keys)
        object.__setattr__(self, 'client_keys', client_keys)
        object.__setattr__(self, 'event_type', GatewayEventType.DEVICE_ATTRIBUTE_REQUEST)
        return self

    @classmethod
    async def from_attribute_request(cls, device_session: DeviceSession, attribute_request: AttributeRequest) -> 'GatewayAttributeRequest':
        """
        Create a GatewayAttributeRequest from an existing AttributeRequest and a DeviceSession.
        """
        if not isinstance(attribute_request, AttributeRequest):
            raise TypeError("attribute_request must be an instance of AttributeRequest")
        self = object.__new__(cls)
        object.__setattr__(self, 'device_session', device_session)
        object.__setattr__(self, 'request_id', attribute_request.request_id)
        object.__setattr__(self, 'shared_keys', attribute_request.shared_keys)
        object.__setattr__(self, 'client_keys', attribute_request.client_keys)
        object.__setattr__(self, 'event_type', GatewayEventType.DEVICE_ATTRIBUTE_REQUEST)
        return self


    def to_payload_format(self) -> Dict[str, Union[str, bool]]:
        """
        Convert the attribute request into the expected MQTT payload format.
        """
        payload = {"device": self.device_session.device_info.device_name, "id": self.request_id}
        single_key_request = (self.client_keys is not None and len(self.client_keys) == 1) or (self.shared_keys is not None and len(self.shared_keys) == 1)
        request_key = 'key' if single_key_request else 'keys'
        if self.client_keys is not None and self.client_keys:
            payload['client'] = True
            payload[request_key] = self.client_keys[0] if single_key_request else self.client_keys
        elif self.shared_keys is not None and self.shared_keys:
            # TODO: In current realisation on server it is not possible to request values for the both scopes simultaneously, recommended to improve the platform API
            payload['client'] = False
            payload[request_key] = self.shared_keys[0] if single_key_request else self.shared_keys
        return payload
