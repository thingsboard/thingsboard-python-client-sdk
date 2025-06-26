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
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from time import monotonic

from tb_mqtt_client.constants.service_keys import MESSAGES_RATE_LIMIT
from tb_mqtt_client.service.mqtt_manager import MQTTManager
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.service.message_dispatcher import MessageDispatcher
from tb_mqtt_client.service.device.handlers.rpc_response_handler import RPCResponseHandler


@pytest_asyncio.fixture
async def setup_manager():
    stop_event = asyncio.Event()
    message_dispatcher = MagicMock(spec=MessageDispatcher)
    on_connect = AsyncMock()
    on_disconnect = AsyncMock()
    on_publish_result = AsyncMock()
    rate_limits_handler = AsyncMock()
    rpc_response_handler = MagicMock(spec=RPCResponseHandler)

    manager = MQTTManager(
        client_id="test-client",
        main_stop_event=stop_event,
        message_dispatcher=message_dispatcher,
        on_connect=on_connect,
        on_disconnect=on_disconnect,
        on_publish_result=on_publish_result,
        rate_limits_handler=rate_limits_handler,
        rpc_response_handler=rpc_response_handler
    )
    return manager, stop_event, message_dispatcher, on_connect, on_disconnect, on_publish_result, rate_limits_handler, rpc_response_handler


@pytest.mark.asyncio
async def test_connect_sets_connect_params(setup_manager):
    manager, *_ = setup_manager
    await manager.connect("localhost", 1883, "user", "pass", tls=False)
    assert manager._connect_params[:4] == ("localhost", 1883, "user", "pass")


@pytest.mark.asyncio
async def test_is_connected_returns_false_if_not_ready(setup_manager):
    manager, *_ = setup_manager
    assert not manager.is_connected()


@pytest.mark.asyncio
async def test_register_and_unregister_handler(setup_manager):
    manager, *_ = setup_manager
    async def dummy(topic, payload): pass
    manager.register_handler("topic/+", dummy)
    assert "topic/+" in manager._handlers
    manager.unregister_handler("topic/+")
    assert "topic/+" not in manager._handlers


@pytest.mark.asyncio
async def test_on_disconnect_internal_abnormal_disconnect(setup_manager):
    manager, *_ = setup_manager

    fut1 = asyncio.Future()
    fut2 = asyncio.Future()
    manager._pending_publishes[101] = (fut1, "topic1", 1, 100, monotonic())
    manager._pending_publishes[102] = (fut2, "topic2", 1, 100, monotonic())

    manager._backpressure = MagicMock()
    manager._backpressure.notify_disconnect = MagicMock()

    manager._on_disconnect_internal(manager._client, reason_code=1)

    assert fut1.done() and fut2.done()

    manager._backpressure.notify_disconnect.assert_called()


@pytest.mark.asyncio
async def test_handle_puback_reason_code_unknown_id(setup_manager):
    manager, *_ = setup_manager
    # Should not raise or fail if ID not tracked
    manager._handle_puback_reason_code(999, 0, {})


@pytest.mark.asyncio
async def test_on_message_internal_handler_exception(setup_manager):
    manager, *_ = setup_manager

    async def bad_handler(topic, payload):
        raise ValueError("oops")

    manager.register_handler("test/topic", bad_handler)
    manager._on_message_internal(manager._client, "test/topic", b"{}", 0, {})
    await asyncio.sleep(0.05)  # Let async task run


def test_match_topic_full_wildcard():
    assert MQTTManager._match_topic("#", "any/depth/of/topic")

@pytest.mark.asyncio
async def test_publish_fails_without_rate_limits(setup_manager):
    manager, *_ = setup_manager
    manager._MQTTManager__rate_limits_retrieved = False
    manager._MQTTManager__is_waiting_for_rate_limits_publish = False
    with pytest.raises(RuntimeError, match="Cannot publish before rate limits are retrieved."):
        await manager.publish("topic", b"payload")


@pytest.mark.asyncio
async def test_publish_force_bypasses_limits(setup_manager):
    manager, *_ = setup_manager
    manager._MQTTManager__rate_limits_retrieved = True
    manager._MQTTManager__is_waiting_for_rate_limits_publish = False
    manager._rate_limits_ready_event.set()

    manager._client._connection = MagicMock()
    manager._client._connection.publish.return_value = (10, b"packet")
    manager._client._persistent_storage = MagicMock()

    result = await manager.publish("topic", b"payload", qos=1, force=True)
    assert isinstance(result, asyncio.Future)


@pytest.mark.asyncio
async def test_on_disconnect_internal_clears_futures(setup_manager):
    manager, *_ = setup_manager
    fut = asyncio.Future()
    manager._pending_publishes[42] = (fut, "topic", 1, 100, monotonic())
    manager._on_disconnect_internal(manager._client, reason_code=0)
    assert not manager._pending_publishes
    assert fut.done()
    assert isinstance(fut.result(), PublishResult)


@pytest.mark.asyncio
async def test_on_message_internal_triggers_handler(setup_manager):
    manager, *_ = setup_manager
    called = asyncio.Event()

    async def dummy_handler(topic, payload):
        called.set()

    manager.register_handler("foo/bar", dummy_handler)
    manager._on_message_internal(manager._client, "foo/bar", b"123", 1, {})
    await asyncio.wait_for(called.wait(), timeout=1)


@pytest.mark.asyncio
async def test_handle_puback_reason_code(setup_manager):
    manager, *_ = setup_manager
    fut = asyncio.Future()
    manager._pending_publishes[123] = (fut, "topic", 1, 100, monotonic())
    manager._handle_puback_reason_code(123, 0, {})
    assert fut.done()
    assert fut.result().message_id == 123


@pytest.mark.asyncio
async def test_await_ready_timeout(setup_manager):
    manager, stop_event, *_ = setup_manager
    with patch("tb_mqtt_client.service.mqtt_manager.await_or_stop", side_effect=asyncio.TimeoutError):
        await manager.await_ready(timeout=0.01)


@pytest.mark.asyncio
async def test_set_rate_limits_allows_ready(setup_manager):
    manager, *_ = setup_manager
    mock_limit = MagicMock()
    manager.set_rate_limits(mock_limit, None, None)
    assert manager._rate_limits_ready_event.is_set()


@pytest.mark.asyncio
async def test_match_topic_logic():
    assert MQTTManager._match_topic("foo/+", "foo/bar")
    assert not MQTTManager._match_topic("foo/bar", "foo/bar/baz")
    assert MQTTManager._match_topic("foo/#", "foo/bar/baz")


@pytest.mark.asyncio
async def test_check_pending_publishes_timeout(setup_manager):
    manager, *_ = setup_manager
    fut = asyncio.Future()
    manager._pending_publishes[1] = (fut, "topic", 1, 100, monotonic() - 20)
    await manager.check_pending_publishes(monotonic())
    assert fut.done()
    assert fut.result().reason_code == 408


@pytest.mark.asyncio
async def test_disconnect_swallows_reset_error(setup_manager):
    manager, *_ = setup_manager
    with patch.object(manager._client, "disconnect", side_effect=ConnectionResetError):
        await manager.disconnect()


@pytest.mark.asyncio
async def test_subscribe_adds_future(setup_manager):
    manager, *_ = setup_manager
    manager._client._connection = MagicMock()
    manager._client._connection.subscribe.return_value = 42

    mock_rate_limit = AsyncMock()
    setattr(manager, "_MQTTManager__rate_limiter", {MESSAGES_RATE_LIMIT: mock_rate_limit})

    fut = await manager.subscribe("topic", qos=1)

    assert 42 in manager._pending_subscriptions
    assert isinstance(fut, asyncio.Future)
    mock_rate_limit.consume.assert_awaited_once()



@pytest.mark.asyncio
async def test_unsubscribe_adds_future(setup_manager):
    manager, *_ = setup_manager
    manager._client._connection = MagicMock()
    manager._client._connection.unsubscribe.return_value = 77

    mock_rate_limit = AsyncMock()
    setattr(manager, "_MQTTManager__rate_limiter", {MESSAGES_RATE_LIMIT: mock_rate_limit})

    fut = await manager.unsubscribe("topic")

    assert 77 in manager._pending_unsubscriptions
    assert isinstance(fut, asyncio.Future)
    mock_rate_limit.consume.assert_awaited_once()


if __name__ == '__main__':
    pytest.main([__file__])