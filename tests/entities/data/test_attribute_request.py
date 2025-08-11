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

from unittest.mock import AsyncMock, patch

import pytest

from tb_mqtt_client.entities.data.attribute_request import AttributeRequest


@pytest.mark.asyncio
@patch("tb_mqtt_client.entities.data.attribute_request.AttributeRequestIdProducer.get_next", new_callable=AsyncMock)
async def test_attribute_request_build_with_keys(mock_get_next):
    mock_get_next.return_value = 101

    request = await AttributeRequest.build(
        shared_keys=["shared1", "shared2"],
        client_keys=["client1"]
    )

    assert request.request_id == 101
    assert request.shared_keys == ["shared1", "shared2"]
    assert request.client_keys == ["client1"]
    assert request.to_payload_format() == {
        "sharedKeys": "shared1,shared2",
        "clientKeys": "client1"
    }
    assert repr(request) == (
        "AttributeRequest(id=101, shared_keys=['shared1', 'shared2'], client_keys=['client1'])"
    )


@pytest.mark.asyncio
@patch("tb_mqtt_client.entities.data.attribute_request.AttributeRequestIdProducer.get_next", new_callable=AsyncMock)
async def test_attribute_request_build_with_none_keys(mock_get_next):
    mock_get_next.return_value = 202

    request = await AttributeRequest.build()
    assert request.request_id == 202
    assert request.shared_keys is None
    assert request.client_keys is None
    assert request.to_payload_format() == {}
    assert repr(request) == "AttributeRequest(id=202, shared_keys=None, client_keys=None)"


@pytest.mark.asyncio
async def test_attribute_request_direct_instantiation_fails():
    with pytest.raises(TypeError, match="Direct instantiation of AttributeRequest is not allowed"):
        AttributeRequest(1, ["s1"], ["c1"])


@pytest.mark.asyncio
async def test_attribute_request_invalid_json_keys():
    with patch("tb_mqtt_client.entities.data.attribute_request.AttributeRequestIdProducer.get_next",
               new_callable=AsyncMock) as mock_get_next:
        mock_get_next.return_value = 999

        with pytest.raises(ValueError, match="unsupported - set"):
            await AttributeRequest.build(shared_keys={1, 2, 3})

        with pytest.raises(ValueError, match="expected str, got int"):
            await AttributeRequest.build(client_keys=[{"bad_key": {1: "value"}}])


@pytest.mark.asyncio
async def test_attribute_request_invalid_nested_value():
    with patch("tb_mqtt_client.entities.data.attribute_request.AttributeRequestIdProducer.get_next",
               new_callable=AsyncMock) as mock_get_next:
        mock_get_next.return_value = 1001

        def dummy(): pass

        with pytest.raises(ValueError, match="unsupported - function"):
            await AttributeRequest.build(client_keys=[{"nested": dummy}])
