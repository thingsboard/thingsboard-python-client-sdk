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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tb_mqtt_client.common.provisioning_client import ProvisioningClient
from tb_mqtt_client.constants.mqtt_topics import PROVISION_RESPONSE_TOPIC
from tb_mqtt_client.constants.provisioning import ProvisioningResponseStatus
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, AccessTokenProvisioningCredentials


@pytest.fixture
def real_request():
    return ProvisioningRequest(
        host="test_host",
        credentials=AccessTokenProvisioningCredentials("key", "secret"),
        port=1883,
        device_name="test_device"
    )


@pytest.mark.asyncio
@patch("tb_mqtt_client.common.provisioning_client.GMQTTClient")
@patch("tb_mqtt_client.common.provisioning_client.JsonMessageAdapter")
async def test_successful_provisioning_flow(mock_dispatcher_cls, mock_gmqtt_cls, real_request):
    mock_client = AsyncMock()
    mock_gmqtt_cls.return_value = mock_client

    mock_dispatcher = MagicMock()
    topic = "provision/topic"
    payload = b'{"provision": "data"}'
    mock_dispatcher.build_provision_request.return_value = (topic, payload)

    mock_device_config = MagicMock()
    mock_dispatcher.parse_provisioning_response.return_value.result = mock_device_config
    mock_dispatcher_cls.return_value = mock_dispatcher

    client = ProvisioningClient("test_host", 1883, real_request)

    client._on_connect(mock_client, None, 0, None)

    mock_client.subscribe.assert_called_once_with(PROVISION_RESPONSE_TOPIC)
    mock_client.publish.assert_called_once_with(topic, payload)

    await client._on_message(None, None, b"payload-data", None, None)

    assert client._device_config == mock_device_config
    assert client._provisioned.is_set()
    mock_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
@patch("tb_mqtt_client.common.provisioning_client.GMQTTClient")
@patch("tb_mqtt_client.common.provisioning_client.JsonMessageAdapter")
async def test_failed_connection(mock_dispatcher_cls, mock_gmqtt_cls, real_request, caplog):
    mock_client = AsyncMock()
    mock_gmqtt_cls.return_value = mock_client

    client = ProvisioningClient("localhost", 1883, real_request)

    with caplog.at_level("ERROR"):
        client._on_connect(mock_client, None, 1, None)

    assert client._device_config is not None
    assert client._device_config.status == ProvisioningResponseStatus.ERROR
    assert client._provisioned.is_set()
    assert "Cannot connect to ThingsBoard!" in caplog.text


@pytest.mark.asyncio
@patch("tb_mqtt_client.common.provisioning_client.GMQTTClient")
@patch("tb_mqtt_client.common.provisioning_client.JsonMessageAdapter")
async def test_provision_method_awaits_provisioned(mock_dispatcher_cls, mock_gmqtt_cls, real_request):
    mock_client = AsyncMock()
    mock_gmqtt_cls.return_value = mock_client

    client = ProvisioningClient("localhost", 1883, real_request)
    expected_config = MagicMock()
    client._device_config = expected_config
    client._provisioned.set()

    result = await client.provision()

    mock_client.connect.assert_awaited_once_with("localhost", 1883)
    assert result == expected_config


def test_initial_state(real_request):
    client = ProvisioningClient("host", 1234, real_request)

    assert client._host == "host"
    assert client._port == 1234
    assert client._provision_request == real_request
    assert client._client_id == "provision"
    assert not client._provisioned.is_set()
    assert client._device_config is None
