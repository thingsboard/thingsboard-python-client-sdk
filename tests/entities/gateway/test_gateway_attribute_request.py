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

from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest


@pytest.mark.asyncio
async def test_direct_instantiation_not_allowed():
    with pytest.raises(TypeError) as exc:
        GatewayAttributeRequest()
    assert "Direct instantiation" in str(exc.value)


@pytest.mark.asyncio
async def test_build_assigns_values_and_repr():
    mock_session = MagicMock()
    mock_session.device_info.device_name = "TestDevice"

    with patch("tb_mqtt_client.common.request_id_generator.AttributeRequestIdProducer.get_next", new=AsyncMock(return_value=123)):
        req = await GatewayAttributeRequest.build(
            device_session=mock_session,
            shared_keys=["s1", "s2"],
            client_keys=["c1"]
        )

    # Check attributes set correctly
    assert req.device_session is mock_session
    assert req.request_id == 123
    assert req.shared_keys == ["s1", "s2"]
    assert req.client_keys == ["c1"]
    assert req.event_type == GatewayEventType.DEVICE_ATTRIBUTE_REQUEST

    # __repr__ should include these values
    r = repr(req)
    assert "TestDevice" in r or "device_session" in r
    assert "shared_keys" in r
    assert "client_keys" in r


@pytest.mark.asyncio
async def test_from_attribute_request_success():
    mock_session = MagicMock()
    mock_session.device_info.device_name = "MyDev"

    attr_req = await AttributeRequest.build(shared_keys=["sh1"], client_keys=["cl1"])

    result = await GatewayAttributeRequest.from_attribute_request(mock_session, attr_req)

    assert result.device_session is mock_session
    assert result.request_id == attr_req.request_id
    assert result.shared_keys == ["sh1"]
    assert result.client_keys == ["cl1"]
    assert result.event_type == GatewayEventType.DEVICE_ATTRIBUTE_REQUEST


@pytest.mark.asyncio
async def test_from_attribute_request_invalid_type():
    mock_session = MagicMock()
    with pytest.raises(TypeError) as exc:
        await GatewayAttributeRequest.from_attribute_request(mock_session, object())
    assert "must be an instance of AttributeRequest" in str(exc.value)


def make_request_for_payload(shared=None, client=None):
    mock_session = MagicMock()
    mock_session.device_info.device_name = "DeviceX"

    req = object.__new__(GatewayAttributeRequest)
    object.__setattr__(req, 'device_session', mock_session)
    object.__setattr__(req, 'request_id', 999)
    object.__setattr__(req, 'shared_keys', shared)
    object.__setattr__(req, 'client_keys', client)
    object.__setattr__(req, 'event_type', GatewayEventType.DEVICE_ATTRIBUTE_REQUEST)
    return req


def test_to_payload_format_client_multiple_keys():
    req = make_request_for_payload(client=["a", "b"])
    payload = req.to_payload_format()
    assert payload["device"] == "DeviceX"
    assert payload["client"] is True
    assert payload["keys"] == ["a", "b"]


def test_to_payload_format_client_single_key():
    req = make_request_for_payload(client=["only_one"])
    payload = req.to_payload_format()
    assert payload["client"] is True
    assert payload["key"] == "only_one"


def test_to_payload_format_shared_multiple_keys():
    req = make_request_for_payload(shared=["s1", "s2"])
    payload = req.to_payload_format()
    assert payload["client"] is False
    assert payload["keys"] == ["s1", "s2"]


def test_to_payload_format_shared_single_key():
    req = make_request_for_payload(shared=["s1"])
    payload = req.to_payload_format()
    assert payload["client"] is False
    assert payload["key"] == "s1"


def test_to_payload_format_no_keys():
    req = make_request_for_payload(shared=None, client=None)
    payload = req.to_payload_format()
    assert set(payload.keys()) == {"device", "id"}


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
