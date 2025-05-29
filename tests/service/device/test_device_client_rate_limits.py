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
from orjson import dumps

from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit


@pytest.fixture
def device_client():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "test_token"
    config.client_id = ""
    return DeviceClient(config)


@pytest.mark.asyncio
async def test_handle_rate_limit_response_valid(device_client):
    payload = {
        "rateLimits": {
            "messages": "10:1,300:60",
            "telemetryMessages": "20:1,600:60",
            "telemetryDataPoints": "100:1,1000:60"
        },
        "maxInflightMessages": 100,
        "maxPayloadSize": 2048
    }

    topic = "v1/devices/me/rpc/response/1"
    await device_client._handle_rate_limit_response(topic, dumps(payload))

    assert isinstance(device_client._messages_rate_limit, RateLimit)
    assert isinstance(device_client._telemetry_rate_limit, RateLimit)
    assert isinstance(device_client._telemetry_dp_rate_limit, RateLimit)

    assert device_client._messages_rate_limit.has_limit()
    assert device_client._telemetry_rate_limit.has_limit()
    assert device_client._telemetry_dp_rate_limit.has_limit()

    assert device_client._max_inflight_messages > 0
    assert device_client._max_queued_messages == device_client._max_inflight_messages
    assert device_client.max_payload_size == int(2048 * device_client._telemetry_rate_limit.percentage / 100)


@pytest.mark.asyncio
async def test_handle_rate_limit_response_invalid_payload(device_client, caplog):
    topic = "v1/devices/me/rpc/response/1"
    await device_client._handle_rate_limit_response(topic, b'invalid-json')

    assert not device_client._messages_rate_limit.has_limit()
    assert "Failed to parse rate limits" in caplog.text


@pytest.mark.asyncio
async def test_handle_rate_limit_response_missing_rate_limits(device_client, caplog):
    payload = {"maxInflightMessages": 100}
    topic = "v1/devices/me/rpc/response/1"
    await device_client._handle_rate_limit_response(topic, dumps(payload))

    assert "Invalid rate limit response" in caplog.text
    assert device_client._max_inflight_messages == 100  # default fallback still applies