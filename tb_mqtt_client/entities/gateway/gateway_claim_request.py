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
from typing import Dict, Any, Union

from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.service.gateway.device_session import DeviceSession


@dataclass(slots=True, frozen=True)
class GatewayClaimRequest(BaseGatewayEvent):

    devices_requests: Dict[Union[DeviceSession, str], ClaimRequest] = None
    event_type: GatewayEventType = GatewayEventType.GATEWAY_CLAIM_REQUEST

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of GatewayClaimRequest is not allowed. Use 'GatewayClaimRequestBuilder.build(...)'.")

    def __repr__(self) -> str:
        return f"GatewayClaimRequest(devices_requests={self.devices_requests})"

    def add_device_request(self, device_name_or_session: Union[DeviceSession, str], claim_request: ClaimRequest):
        """
        Add a device claim request to the GatewayClaimRequest.
        """
        self.devices_requests[device_name_or_session] = claim_request

    def to_payload_format(self) -> Dict[str, Any]:
        """
        Convert the claim request to the expected MQTT JSON payload format.
        """
        payload = {}
        for device_session, claim_request in self.devices_requests.items():
            device_name = device_session
            if isinstance(device_session, DeviceSession):
                device_name = device_session.device_info.device_name
            payload[device_name] = claim_request.to_payload_format()
        return payload

    @classmethod
    def build(cls) -> 'GatewayClaimRequest':
        """
        Build a new GatewayClaimRequest instance.
        """
        self = object.__new__(cls)
        object.__setattr__(self, 'devices_requests', {})
        object.__setattr__(self, 'event_type', GatewayEventType.GATEWAY_CLAIM_REQUEST)
        return self


class GatewayClaimRequestBuilder:
    """
    Builder class for GatewayClaimRequest.
    Allows adding multiple device claim requests in a fluent interface style.
    """
    def __init__(self):
        self._devices_requests: Dict[Union[DeviceSession, str], ClaimRequest] = {}

    def add_device_request(self, device_name_or_session: Union[DeviceSession, str], device_claim_request: ClaimRequest) -> 'GatewayClaimRequestBuilder':
        """
        Add a device claim request to the builder.
        """
        if not isinstance(device_name_or_session, (DeviceSession, str)):
            raise ValueError("device_session must be an instance of DeviceSession or a string representing the device name")
        if not isinstance(device_claim_request, ClaimRequest):
            raise ValueError("device_claim_request must be an instance of ClaimRequest")
        self._devices_requests[device_name_or_session] = device_claim_request
        return self

    def build(self) -> GatewayClaimRequest:
        """
        Build the GatewayClaimRequest with all added device requests.
        """
        gateway_claim_request = GatewayClaimRequest.build()
        for device_session, claim_request in self._devices_requests.items():
            gateway_claim_request.add_device_request(device_session, claim_request)
        return gateway_claim_request