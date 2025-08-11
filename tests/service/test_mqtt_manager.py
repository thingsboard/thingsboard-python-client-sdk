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
import ssl
from time import monotonic
from unittest.mock import AsyncMock, MagicMock, patch, call, PropertyMock

import pytest
import pytest_asyncio

from tb_mqtt_client.common.exceptions import BackpressureException
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.common.rate_limit.rate_limiter import RateLimiter
from tb_mqtt_client.service.device.handlers.rpc_response_handler import RPCResponseHandler
from tb_mqtt_client.service.device.message_adapter import MessageAdapter
from tb_mqtt_client.service.mqtt_manager import MQTTManager, IMPLEMENTATION_SPECIFIC_ERROR, QUOTA_EXCEEDED


@pytest_asyncio.fixture
async def setup_manager():
    stop_event = asyncio.Event()
    message_adapter = MagicMock(spec=MessageAdapter)
    on_connect = AsyncMock()
    on_disconnect = AsyncMock()
    on_publish_result = AsyncMock()
    rate_limits_handler = AsyncMock()
    rpc_response_handler = MagicMock(spec=RPCResponseHandler)

    manager = MQTTManager(
        client_id="test-client",
        main_stop_event=stop_event,
        message_adapter=message_adapter,
        on_connect=on_connect,
        on_disconnect=on_disconnect,
        on_publish_result=on_publish_result,
        rate_limits_handler=rate_limits_handler,
        rpc_response_handler=rpc_response_handler
    )
    return (manager, stop_event, message_adapter, on_connect, on_disconnect,
            on_publish_result, rate_limits_handler, rpc_response_handler)


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
    manager._pending_publishes[101] = (fut1, MqttPublishMessage("topic1", b"test"), monotonic())
    manager._pending_publishes[102] = (fut2, MqttPublishMessage("topic2", b"test"), monotonic())

    manager._backpressure = MagicMock()
    manager._backpressure.notify_disconnect = MagicMock()

    manager._on_disconnect_internal(manager._client, reason_code=1)

    assert fut1.done() and fut2.done()

    manager._backpressure.notify_disconnect.assert_called()


@pytest.mark.asyncio
async def test_handle_puback_reason_code_unknown_id(setup_manager):
    manager, *_ = setup_manager
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
        mqtt_message = MqttPublishMessage("topic", b"payload")
        await manager.publish(mqtt_message, force=False)


@pytest.mark.asyncio
async def test_publish_force_bypasses_limits(setup_manager):
    manager, *_ = setup_manager
    manager._MQTTManager__rate_limits_retrieved = True
    manager._MQTTManager__is_waiting_for_rate_limits_publish = False
    manager._rate_limits_ready_event.set()

    manager._client._connection = MagicMock()
    manager._client._connection.publish.return_value = (10, b"packet")
    manager._client._persistent_storage = MagicMock()

    mqtt_publish_message = MqttPublishMessage("topic", b"payload", qos=1)
    await manager.publish(mqtt_publish_message, force=True)
    assert manager._client._connection.publish.call_count == 1


@pytest.mark.asyncio
async def test_publish_backpressure_blocks(setup_manager):
    manager, *_ = setup_manager
    manager._MQTTManager__rate_limits_retrieved = True
    manager._MQTTManager__is_waiting_for_rate_limits_publish = False
    manager._rate_limits_ready_event.set()
    manager._backpressure = MagicMock()
    manager._backpressure.should_pause.return_value = True

    with pytest.raises(BackpressureException):
        await manager.publish(MqttPublishMessage("t", b"x"), force=False)


@pytest.mark.asyncio
async def test_on_disconnect_internal_clears_futures(setup_manager):
    manager, *_ = setup_manager
    fut = asyncio.Future()
    manager._pending_publishes[42] = (fut, MqttPublishMessage("topic", b"payload"), monotonic())
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
    fut.uuid = "test-future"
    manager._pending_publishes[123] = (fut, MqttPublishMessage("topic", b"payload", delivery_futures=[fut]),
                                       monotonic())
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
    manager.set_rate_limits_received()
    assert manager._rate_limits_ready_event.is_set()


@pytest.mark.asyncio
async def test_set_rate_limits_gateway_requires_gateway_limits(setup_manager):
    manager, *_ = setup_manager
    manager.enable_gateway_mode()
    manager.set_rate_limits_received()
    assert not manager._rate_limits_ready_event.is_set()
    manager.set_gateway_rate_limits_received()
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
    manager._pending_publishes[1] = (fut, MqttPublishMessage("topic", b"payload"), monotonic() - 20)
    await manager.check_pending_publishes(monotonic())
    assert fut.done()
    assert fut.result().reason_code == 408


@pytest.mark.asyncio
async def test_check_pending_publishes_cancelled_when_stopping(setup_manager):
    manager, stop_event, *_ = setup_manager
    fut = asyncio.Future()
    manager._pending_publishes[5] = (fut, MqttPublishMessage("t", b"p"), monotonic())
    stop_event.set()
    await manager.check_pending_publishes(monotonic())
    assert fut.cancelled()
    stop_event.clear()


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
    rate_limiter = RateLimiter(mock_rate_limit, MagicMock(), MagicMock())
    setattr(manager, "_MQTTManager__rate_limiter", rate_limiter)

    fut = await manager.subscribe("topic", qos=1)
    await asyncio.sleep(0.1)

    assert 42 in manager._pending_subscriptions
    assert isinstance(fut, asyncio.Future)
    mock_rate_limit.consume.assert_awaited_once()


@pytest.mark.asyncio
async def test_unsubscribe_adds_future(setup_manager):
    manager, *_ = setup_manager
    manager._client._connection = MagicMock()
    manager._client._connection.unsubscribe.return_value = 77

    mock_rate_limit = AsyncMock()
    rate_limiter = RateLimiter(mock_rate_limit, MagicMock(), MagicMock())
    setattr(manager, "_MQTTManager__rate_limiter", rate_limiter)

    fut = await manager.unsubscribe("topic")
    await asyncio.sleep(0.1)

    assert 77 in manager._pending_unsubscriptions
    assert isinstance(fut, asyncio.Future)
    mock_rate_limit.consume.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_qos_zero_sets_result_immediately(setup_manager):
    manager, *_ = setup_manager
    manager._MQTTManager__rate_limits_retrieved = True
    manager._MQTTManager__is_waiting_for_rate_limits_publish = False
    manager._rate_limits_ready_event.set()

    manager._client._connection = MagicMock()
    manager._client._connection.publish.return_value = (99, b"packet")
    manager._client._persistent_storage = MagicMock()
    future = asyncio.Future()

    await manager.publish(MqttPublishMessage("topic", b"payload", qos=0, delivery_futures=future), force=True)
    await asyncio.sleep(0.05)
    assert future.done()
    assert future.result() == PublishResult("topic", 0, -1, 7, 0)


@pytest.mark.asyncio
async def test_on_subscribe_internal_sets_future(setup_manager):
    manager, *_ = setup_manager
    future = asyncio.Future()
    manager._pending_subscriptions[5] = future
    manager._on_subscribe_internal(manager._client, 5, 1, {})
    assert future.done()
    assert future.result() == 5


@pytest.mark.asyncio
async def test_on_unsubscribe_internal_sets_future(setup_manager):
    manager, *_ = setup_manager
    future = asyncio.Future()
    manager._pending_unsubscriptions[11] = future
    manager._on_unsubscribe_internal(manager._client, 11, {})
    assert future.done()
    assert future.result() == 11


@pytest.mark.asyncio
async def test_handle_puback_reason_code_errors(setup_manager):
    manager, *_ = setup_manager

    f1 = asyncio.Future()
    manager._pending_publishes[1] = (f1, MqttPublishMessage("topic", b"payload"), 0)
    manager._handle_puback_reason_code(1, IMPLEMENTATION_SPECIFIC_ERROR, {})
    assert f1.result().reason_code == IMPLEMENTATION_SPECIFIC_ERROR

    f2 = asyncio.Future()
    manager._pending_publishes[2] = (f2, MqttPublishMessage("topic", b"payload"), 0)
    manager._handle_puback_reason_code(2, QUOTA_EXCEEDED, {})
    assert f2.result().reason_code == QUOTA_EXCEEDED

    manager._handle_puback_reason_code(9999, 1, {})


@pytest.mark.asyncio
async def test_request_rate_limits_timeout(setup_manager):
    manager, stop_event, _, _, _, _, rate_handler, _ = setup_manager
    adapter = manager._message_adapter

    req_mock = MagicMock()
    req_mock.request_id = "req-id"

    adapter.build_rpc_request.return_value = MqttPublishMessage("topic", b"payload")

    manager._client._connection = MagicMock()
    manager._client._connection.publish.return_value = (999, b"fake_packet")
    manager._client._persistent_storage = MagicMock()
    manager._client._persistent_storage.push_message_nowait = MagicMock()

    future = asyncio.Future()
    future.set_result(None)
    manager._rpc_response_handler.register_request.return_value = future

    with patch("tb_mqtt_client.entities.data.rpc_request.RPCRequest.build", return_value=req_mock):
        await manager._MQTTManager__request_rate_limits()
        manager.set_rate_limits_received()
        assert manager._rate_limits_ready_event.is_set()


@pytest.mark.asyncio
async def test_request_rate_limits_real_timeout_branch(setup_manager):
    manager, *_ = setup_manager
    manager._MQTTManager__is_waiting_for_rate_limits_publish = False

    manager._message_adapter.build_rpc_request.return_value = MqttPublishMessage("t", b"p")
    manager._client._connection = MagicMock()
    manager._client._connection.publish.return_value = (1, b"p")
    manager._client._persistent_storage = MagicMock()

    never = asyncio.Future()
    manager._rpc_response_handler.register_request.return_value = never

    with patch("tb_mqtt_client.service.mqtt_manager.await_or_stop", side_effect=asyncio.TimeoutError):
        await manager._MQTTManager__request_rate_limits()

    assert manager._MQTTManager__is_waiting_for_rate_limits_publish is True
    assert not manager._rate_limits_ready_event.is_set()


@pytest.mark.asyncio
async def test_monitor_ack_timeouts_stops_gracefully(setup_manager):
    manager, stop_event, *_ = setup_manager
    stop_event.set()
    await manager._monitor_ack_timeouts()
    stop_event.clear()


@pytest.mark.asyncio
async def test_match_topic_exact_match_and_failures():
    assert MQTTManager._match_topic("a/b/c", "a/b/c")
    assert not MQTTManager._match_topic("a/b/c", "a/b")
    assert not MQTTManager._match_topic("a/+/c", "a/x")


@patch("tb_mqtt_client.service.mqtt_manager.run_coroutine_sync")
@pytest.mark.asyncio
async def test_disconnect_reason_code_142_triggers_special_flow(mock_run_sync, setup_manager):
    mock_run_sync.return_value = (None, 1, 1)
    manager, *_ = setup_manager
    manager._client = MagicMock()
    manager._backpressure = MagicMock()
    manager._on_disconnect_callback = AsyncMock()
    manager._run_coroutine_sync = MagicMock(return_value=(None, 1, 1))

    rate_limit = MagicMock(spec=RateLimit)
    manager._MQTTManager__rate_limiter = {"messages": rate_limit}

    fut = asyncio.Future()
    manager._pending_publishes[42] = (fut, MqttPublishMessage("topic", b"payload"), 0)

    manager._on_disconnect_internal(manager._client, reason_code=142)
    await asyncio.sleep(0.05)

    assert fut.done()
    manager._backpressure.notify_disconnect.assert_has_calls([
        call(delay_seconds=10),
        call(delay_seconds=1),
    ])
    manager._on_disconnect_callback.assert_awaited_once()


@pytest.mark.asyncio
async def test_duplicate_publish_path_retransmits_and_persists(setup_manager):
    manager, *_ = setup_manager
    conn = MagicMock()
    protocol = object()
    conn._protocol = protocol
    manager._client._connection = conn
    manager._client._persistent_storage = MagicMock()

    msg = MqttPublishMessage("topic/dup", b"payload", qos=1)
    msg.dup = True
    msg.message_id = 321

    with patch("tb_mqtt_client.service.mqtt_manager.PublishPacket.build_package",
               return_value=(321, b"rebuilt")) as mock_build:
        await manager.publish(msg, force=True)

    mock_build.assert_called_once()
    conn.send_package.assert_called_once_with(b"rebuilt")
    manager._client._persistent_storage.push_message_nowait.assert_called_once_with(321, msg)


@pytest.mark.asyncio
async def test_add_future_chain_processing_success_and_immediate(setup_manager):
    manager, *_ = setup_manager
    main = asyncio.get_event_loop().create_future()
    main.uuid = "main"

    f1 = asyncio.get_event_loop().create_future()
    f1.uuid = "f1"
    f2 = asyncio.get_event_loop().create_future()
    f2.uuid = "f2"

    msg = MqttPublishMessage("t", b"p", qos=1, delivery_futures=[f1, f2])

    with patch("tb_mqtt_client.service.mqtt_manager.future_map.child_resolved") as child_resolved:
        await MQTTManager._add_future_chain_processing(main, msg)
        res = PublishResult("t", 1, -1, 1, 0)
        main.set_result(res)
        await asyncio.sleep(0.05)
        assert f1.done() and f2.done()
        assert f1.result() == res and f2.result() == res
        assert child_resolved.call_count == 2

        main2 = asyncio.get_event_loop().create_future()
        main2.uuid = "main2"
        main2.set_result(res)
        g1 = asyncio.get_event_loop().create_future(); g1.uuid = "g1"
        g2 = asyncio.get_event_loop().create_future(); g2.uuid = "g2"
        msg2 = MqttPublishMessage("t2", b"q", qos=1, delivery_futures=[g1, g2])
        await MQTTManager._add_future_chain_processing(main2, msg2)
        await asyncio.sleep(0.05)
        assert g1.done() and g2.done()



@pytest.mark.asyncio
async def test_add_future_chain_processing_cancel_and_exception(setup_manager):
    manager, *_ = setup_manager

    cancelled_main = asyncio.get_event_loop().create_future()
    cancelled_main.uuid = "cmain"
    cancelled_main.cancel()
    c1 = asyncio.get_event_loop().create_future(); c1.uuid = "c1"
    c2 = asyncio.get_event_loop().create_future(); c2.uuid = "c2"
    msg = MqttPublishMessage("t", b"p", qos=1, delivery_futures=[c1, c2])

    with patch("tb_mqtt_client.service.mqtt_manager.future_map.child_resolved"):
        await MQTTManager._add_future_chain_processing(cancelled_main, msg)
        await asyncio.sleep(0.05)
        assert c1.done() and c2.done()
        assert isinstance(c1.result(), PublishResult) and isinstance(c2.result(), PublishResult)

    exc_main = asyncio.get_event_loop().create_future()
    exc_main.uuid = "emain"
    exc_main.set_exception(RuntimeError("boom"))
    e1 = asyncio.get_event_loop().create_future(); e1.uuid = "e1"
    e2 = asyncio.get_event_loop().create_future(); e2.uuid = "e2"
    msg2 = MqttPublishMessage("t2", b"p2", qos=1, delivery_futures=[e1, e2])

    with patch("tb_mqtt_client.service.mqtt_manager.future_map.child_resolved"):
        await MQTTManager._add_future_chain_processing(exc_main, msg2)
        await asyncio.sleep(0.05)
        assert e1.done() and e2.done()
        assert isinstance(e1.result(), PublishResult) and isinstance(e2.result(), PublishResult)


@pytest.mark.asyncio
async def test_process_regular_publish_qos1_resolves_delivery_on_puback(setup_manager):
    manager, *_ = setup_manager
    manager._client._connection = MagicMock()
    manager._client._connection.publish.return_value = (555, b"p")
    manager._client._persistent_storage = MagicMock()

    d1 = asyncio.get_event_loop().create_future();
    d1.uuid = "d1"
    d2 = asyncio.get_event_loop().create_future();
    d2.uuid = "d2"
    msg = MqttPublishMessage("t", b"payload", qos=1, delivery_futures=[d1, d2])
    await manager.publish(msg, force=True)

    manager._handle_puback_reason_code(555, 0, {})
    await asyncio.sleep(0.05)
    assert d1.done() and d2.done()
    assert isinstance(d1.result(), PublishResult) and isinstance(d2.result(), PublishResult)


@pytest.mark.asyncio
async def test_on_connect_internal_failure_does_not_set_ready(setup_manager):
    manager, *_ = setup_manager
    manager._connected_event.set()
    manager._on_connect_internal(manager._client, session_present=False, reason_code=128, properties=None)
    assert not manager._connected_event.is_set()



@pytest.mark.asyncio
async def test_on_connect_internal_success_triggers_limits_flow(setup_manager):
    manager, *_ = setup_manager
    called = asyncio.Event()

    async def fake_limits_flow():
        called.set()

    manager._client._connection = type("Conn", (), {})()

    setattr(manager, "_MQTTManager__handle_connect_and_limits", fake_limits_flow)

    manager._on_connect_internal(manager._client, session_present=True, reason_code=0, properties={})
    await asyncio.wait_for(called.wait(), timeout=1.0)
    assert manager._connected_event.is_set()


@pytest.mark.asyncio
async def test_patch_client_for_retry_logic_assigns_method(setup_manager):
    manager, *_ = setup_manager

    async def put_retry(msg: MqttPublishMessage):
        return None

    manager.patch_client_for_retry_logic(put_retry)
    assert manager._client.put_retry_message is put_retry


@pytest.mark.asyncio
async def test_stop_cancels_tasks_and_calls_patch_utils(setup_manager):
    manager, *_ = setup_manager
    manager._patch_utils.stop_retry_task = AsyncMock()

    with patch.object(type(manager._client), "is_connected", new_callable=PropertyMock, return_value=False):
        await manager.stop()

    manager._patch_utils.stop_retry_task.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_tls_creates_default_ssl_context(setup_manager):
    manager, *_ = setup_manager

    manager._client.connect = AsyncMock()

    with patch.object(type(manager._client), "is_connected", new_callable=PropertyMock, return_value=True), \
         patch.object(ssl, "create_default_context", autospec=True) as create_ctx:
        await manager.connect("host", 8883, tls=True, ssl_context=None)

    create_ctx.assert_called_once()


@pytest.mark.asyncio
async def test_connect_loop_handles_wait_exception_and_exits_when_connected(setup_manager):
    manager, stop_event, *_ = setup_manager

    manager._client.connect = AsyncMock()

    with patch.object(type(manager._client), "is_connected", new_callable=PropertyMock,
                      side_effect=[False, True]), \
         patch.object(manager._connected_event, "wait",
                      AsyncMock(side_effect=[Exception("boom"), None])):
        await manager.connect("host", 1883, tls=False)


@pytest.mark.asyncio
async def test_publish_wait_rate_limits_timeout_requests_then_raises(setup_manager):
    manager, *_ = setup_manager

    manager._MQTTManager__rate_limits_retrieved = True
    manager._MQTTManager__is_waiting_for_rate_limits_publish = False
    manager._backpressure.should_pause = MagicMock(return_value=False)

    with patch("tb_mqtt_client.service.mqtt_manager.await_or_stop", side_effect=asyncio.TimeoutError), \
         patch.object(manager, "_MQTTManager__request_rate_limits", new=AsyncMock()) as req_limits:
        with pytest.raises(RuntimeError, match="Timeout waiting for rate limits."):
            await manager.publish(MqttPublishMessage("t", b"p"), force=False)

    req_limits.assert_awaited_once()


@pytest.mark.asyncio
async def test_disconnect_rate_limit_timeout_sets_default_backpressure_delay(setup_manager):
    manager, *_ = setup_manager

    rate_limit = MagicMock(spec=RateLimit)
    setattr(manager, "_MQTTManager__rate_limiter", {"messages": rate_limit})
    manager._backpressure = MagicMock()

    with patch("tb_mqtt_client.service.mqtt_manager.run_coroutine_sync", side_effect=TimeoutError):
        manager._on_disconnect_internal(manager._client, reason_code=131)

    manager._backpressure.notify_disconnect.assert_called_with(delay_seconds=10)


@pytest.mark.asyncio
async def test_add_future_chain_processing_sets_exception_when_child_resolved_raises(setup_manager):
    manager, *_ = setup_manager

    main = asyncio.get_event_loop().create_future()
    main.uuid = "main"

    f_ok = asyncio.get_event_loop().create_future(); f_ok.uuid = "ok"
    f_err = asyncio.get_event_loop().create_future(); f_err.uuid = "err"
    msg = MqttPublishMessage("topic", b"payload", qos=1, delivery_futures=[f_ok, f_err])

    with patch("tb_mqtt_client.service.mqtt_manager.future_map.child_resolved",
               side_effect=RuntimeError("boom")):
        await MQTTManager._add_future_chain_processing(main, msg)

        main.set_result(PublishResult("topic", 1, -1, len(b"payload"), 0))
        await asyncio.sleep(0.05)

    assert f_ok.done()
    assert isinstance(f_ok.result(), PublishResult)

    assert f_err.done()
    assert isinstance(f_err.exception(), Exception)


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
