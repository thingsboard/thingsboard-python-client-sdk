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

import asyncio
import pytest
from unittest.mock import MagicMock

from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.service.mqtt_manager import MQTTManager
from gmqtt import Message


@pytest.fixture
def mqtt_manager():
    manager = MQTTManager("test-client")
    manager._client._connection = MagicMock()
    manager._client._persistent_storage = MagicMock()
    return manager


@pytest.mark.asyncio
async def test_publish_acknowledgment(mqtt_manager):
    fake_mid = 101
    message = Message("v1/devices/me/telemetry", b'{"temp":25}', qos=1)

    mqtt_manager._client._connection.publish = MagicMock(return_value=(fake_mid, b"fake_package"))
    mqtt_manager.set_rate_limits(message_rate_limit=RateLimit("0:0"),
                                 telemetry_message_rate_limit=None,
                                 telemetry_dp_rate_limit=None)

    future = await mqtt_manager.publish(message)

    assert fake_mid in mqtt_manager._pending_publishes
    mqtt_manager._on_publish_internal(mqtt_manager._client, fake_mid)

    result = await future
    assert result is True
    assert fake_mid not in mqtt_manager._pending_publishes


@pytest.mark.asyncio
async def test_subscribe_acknowledgment(mqtt_manager):
    fake_mid = 202
    mqtt_manager._client._connection.subscribe = MagicMock(return_value=fake_mid)

    future = await mqtt_manager.subscribe("v1/devices/me/rpc/request/+")

    assert fake_mid in mqtt_manager._pending_subscriptions
    mqtt_manager._on_subscribe_internal(mqtt_manager._client, fake_mid, 1, None)

    result = await future
    assert result is True
    assert fake_mid not in mqtt_manager._pending_subscriptions


@pytest.mark.asyncio
async def test_unsubscribe_acknowledgment(mqtt_manager):
    fake_mid = 303
    mqtt_manager._client._connection.unsubscribe = MagicMock(return_value=fake_mid)

    future = await mqtt_manager.unsubscribe("v1/devices/me/rpc/request/+")

    assert fake_mid in mqtt_manager._pending_unsubscriptions
    mqtt_manager._on_unsubscribe_internal(mqtt_manager._client, fake_mid)

    result = await future
    assert result is True
    assert fake_mid not in mqtt_manager._pending_unsubscriptions


@pytest.mark.asyncio
async def test_topic_handler_matching(mqtt_manager):
    called = asyncio.Event()

    async def handler(topic, payload):
        assert topic == "v1/devices/me/rpc/request/42"
        assert payload == b"payload"
        called.set()

    mqtt_manager.register_handler("v1/devices/me/rpc/request/+", handler)

    mqtt_manager._on_message_internal(
        mqtt_manager._client,
        topic="v1/devices/me/rpc/request/42",
        payload=b"payload",
        qos=1,
        properties=None
    )

    await asyncio.wait_for(called.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_global_rate_limit_allows_publish(mqtt_manager):
    rate_limiter = RateLimit("2:10")
    mqtt_manager.set_rate_limits(rate_limiter,
                                 telemetry_message_rate_limit=None,
                                 telemetry_dp_rate_limit=None)

    mqtt_manager._client._connection.publish = MagicMock(return_value=(1002, b"pkg"))
    future = await mqtt_manager.publish("topic", b'data')

    assert isinstance(future, asyncio.Future)


@pytest.mark.asyncio
async def test_gateway_rate_limit_per_device_allows(mqtt_manager):
    rl_a = RateLimit("2:60")
    rl_b = RateLimit("5:60")

    mqtt_manager.set_rate_limits({"deviceA": rl_a, "deviceB": rl_b},
                                 telemetry_message_rate_limit=None,
                                 telemetry_dp_rate_limit=None)
    mqtt_manager._client._connection.publish = MagicMock(return_value=(1004, b"pkg"))

    future = await mqtt_manager.publish("topic", b'data')
    assert isinstance(future, asyncio.Future)


@pytest.mark.asyncio
async def test_publish_before_rate_limit_not_retrieved(mqtt_manager):
    mqtt_manager._client._connection.publish = MagicMock(return_value=(1005, b"pkg"))
    mqtt_manager._client._persistent_storage = MagicMock()

    # Should raise unless it's the special rate-limit request
    with pytest.raises(RuntimeError, match="Cannot publish before rate limits"):
        await mqtt_manager.publish("v1/devices/me/telemetry", b'data')


@pytest.mark.asyncio
async def test_publish_during_rate_limit_request_allowed(mqtt_manager):
    # Simulate internal state for initial rate limit request
    mqtt_manager._client._connection.publish = MagicMock(return_value=(1006, b"pkg"))
    mqtt_manager._client._persistent_storage = MagicMock()
    mqtt_manager._MQTTManager__is_waiting_for_rate_limits_publish = True
    mqtt_manager._rate_limits_ready_event.set()

    future = await mqtt_manager.publish("v1/devices/me/rpc/request/1", b'data')
    assert isinstance(future, asyncio.Future)