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

import pytest

from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_claim_request import (
    GatewayClaimRequest,
    GatewayClaimRequestBuilder
)
from tb_mqtt_client.service.gateway.device_session import DeviceSession


def test_direct_instantiation_not_allowed():
    with pytest.raises(TypeError) as exc:
        GatewayClaimRequest()
    assert "Direct instantiation" in str(exc.value)


def test_repr_and_add_device_request_with_str():
    claim_req = ClaimRequest.build("secret", 1000)

    req = GatewayClaimRequest.build()
    assert isinstance(req.devices_requests, dict)
    assert req.event_type == GatewayEventType.GATEWAY_CLAIM_REQUEST

    req.add_device_request("device_1", claim_req)
    assert "device_1" in req.devices_requests
    assert repr(req) == f"GatewayClaimRequest(devices_requests={req.devices_requests})"

    payload = req.to_payload_format()
    assert payload == {"device_1": claim_req.to_payload_format()}


def test_add_device_request_with_device_session():
    claim_req = ClaimRequest.build("key2", 2000)
    dev_info = DeviceInfo(device_name="devA", device_profile="default")
    dev_session = DeviceSession(dev_info)

    req = GatewayClaimRequest.build()
    req.add_device_request(dev_session, claim_req)

    payload = req.to_payload_format()
    assert payload == {"devA": claim_req.to_payload_format()}


def test_builder_add_and_build_with_str():
    claim_req = ClaimRequest.build("builder_secret", 3000)
    builder = GatewayClaimRequestBuilder()
    builder.add_device_request("devB", claim_req)

    built_req = builder.build()
    assert isinstance(built_req, GatewayClaimRequest)
    assert built_req.to_payload_format() == {"devB": claim_req.to_payload_format()}


def test_builder_add_and_build_with_device_session():
    claim_req = ClaimRequest.build("builder_secret_2", 4000)
    dev_info = DeviceInfo(device_name="devC", device_profile="default")
    dev_session = DeviceSession(dev_info)

    builder = GatewayClaimRequestBuilder()
    builder.add_device_request(dev_session, claim_req)

    built_req = builder.build()
    assert built_req.to_payload_format() == {"devC": claim_req.to_payload_format()}


@pytest.mark.parametrize("bad_device", [123, object()])
def test_builder_add_device_request_invalid_device_type(bad_device):
    builder = GatewayClaimRequestBuilder()
    with pytest.raises(ValueError) as exc:
        builder.add_device_request(bad_device, ClaimRequest.build("key", 100))
    assert "DeviceSession or a string" in str(exc.value)


@pytest.mark.parametrize("bad_claim", [123, object(), "string"])
def test_builder_add_device_request_invalid_claim_request(bad_claim):
    builder = GatewayClaimRequestBuilder()
    with pytest.raises(ValueError) as exc:
        builder.add_device_request("devName", bad_claim)
    assert "must be an instance of ClaimRequest" in str(exc.value)


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
