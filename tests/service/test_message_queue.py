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
from unittest.mock import Mock, MagicMock

import pytest
import pytest_asyncio

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.common.rate_limit.backpressure_controller import BackpressureController
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.common.rate_limit.rate_limiter import RateLimiter
from tb_mqtt_client.constants.mqtt_topics import DEVICE_TELEMETRY_TOPIC
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessageBuilder
from tb_mqtt_client.service.device.message_adapter import JsonMessageAdapter
from tb_mqtt_client.service.message_queue import MessageQueue


@pytest_asyncio.fixture
async def fake_mqtt_manager():
    mgr = AsyncMock()
    mgr.backpressure = Mock(spec=BackpressureController)
    mgr.backpressure.should_pause.return_value = False
    return mgr


@pytest_asyncio.fixture
async def message_queue(fake_mqtt_manager):
    main_stop_event = asyncio.Event()
    message_adapter = Mock()
    message_adapter.build_uplink_messages.return_value = []

    mq = MessageQueue(
        mqtt_manager=fake_mqtt_manager,
        main_stop_event=main_stop_event,
        device_rate_limiter=RateLimiter(RateLimit('0:0,'), RateLimit('0:0,'), RateLimit('0:0,')),
        message_adapter=message_adapter,
        max_queue_size=10,
        batch_collect_max_time_ms=10,
        batch_collect_max_count=5
    )

    try:
        yield mq
    finally:
        await mq.shutdown()


@pytest.mark.asyncio
async def test_publish_success(message_queue):
    message = MqttPublishMessage("topic", b"data", qos=1, delivery_futures=[])
    await message_queue.publish(message)
    assert not message_queue.is_empty()


@pytest.mark.asyncio
async def test_publish_queue_full(message_queue):
    for _ in range(10):
        await message_queue.publish(MqttPublishMessage("topic", b"data", qos=1, delivery_futures=[]))
    message = MqttPublishMessage("topic", b"data", qos=1, delivery_futures=[Mock()])
    await message_queue.publish(message)  # Should not raise
    assert message_queue._queue.qsize() <= 10


@pytest.mark.asyncio
async def test_shutdown_clears_tasks_and_queue(fake_mqtt_manager):
    q = MessageQueue(
        mqtt_manager=fake_mqtt_manager,
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=Mock(build_uplink_messages=Mock(return_value=[])),
        max_queue_size=10,
        batch_collect_max_time_ms=10,
        batch_collect_max_count=5
    )
    await q.publish(MqttPublishMessage("topic", b"data", qos=1, delivery_futures=[]))
    await q.shutdown()
    assert q.is_empty()


@pytest.mark.asyncio
async def test_try_publish_backpressure_delay(message_queue):
    message_queue._mqtt_manager.backpressure.should_pause.return_value = True

    built = DeviceUplinkMessageBuilder().add_timeseries(
        TimeseriesEntry("test", "test")
    ).build()
    message = MqttPublishMessage("topic", built, qos=1, delivery_futures=[])

    # Patch the retry scheduler to bypass delay and requeue immediately
    with patch.object(message_queue, "_schedule_delayed_retry") as mocked_retry:
        mocked_retry.side_effect = lambda m: message_queue._queue.put_nowait(m)

        await message_queue._try_publish(message)

        # Wait briefly for the message to be re-enqueued
        for _ in range(20):
            if not message_queue.is_empty():
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("Message was not re-enqueued in time")

        # Ensure _schedule_delayed_retry was triggered
        assert mocked_retry.called, "_schedule_delayed_retry should be called"


@pytest.mark.asyncio
async def test_try_publish_rate_limit_triggered():
    limit = Mock(spec=RateLimit)
    limit.try_consume = AsyncMock(return_value=(10, 1))
    limit.minimal_timeout = 0.01

    mq = MessageQueue(
        mqtt_manager=AsyncMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(AsyncMock(), limit, AsyncMock()),
        message_adapter=Mock(build_uplink_messages=Mock(return_value=[])),
    )
    mq._schedule_delayed_retry = AsyncMock()
    mq._mqtt_manager.publish = AsyncMock()
    message = MqttPublishMessage("telemetry", b"{}", qos=1, delivery_futures=[])
    await mq._try_publish(message)
    await asyncio.sleep(0.05)
    assert mq._mqtt_manager.publish.call_count == 0
    assert mq._schedule_delayed_retry.call_count == 1


@pytest.mark.asyncio
async def test_try_publish_failure_schedules_retry(message_queue):
    message = MqttPublishMessage("topic", b"broken", qos=1, delivery_futures=[])
    message_queue._mqtt_manager.publish.side_effect = Exception("fail")
    message_queue._schedule_delayed_retry = AsyncMock()
    message_queue._mqtt_manager._backpressure.should_pause.return_value = False
    await message_queue._try_publish(message)
    await asyncio.sleep(0.2)
    assert message_queue._schedule_delayed_retry.call_count == 1


@pytest.mark.asyncio
async def test_cancel_tasks():
    task1 = asyncio.create_task(asyncio.sleep(5))
    task2 = asyncio.create_task(asyncio.sleep(5))
    tasks = {task1, task2}
    await MessageQueue._cancel_tasks(tasks)
    assert all(t.cancelled() or t.done() for t in [task1, task2])


@pytest.mark.asyncio
async def test_rate_limit_refill():
    rate_limit = Mock(spec=RateLimit)
    rate_limit.refill = AsyncMock()
    rate_limit.to_dict = Mock(return_value={"x": 1})
    mq = MessageQueue(
        mqtt_manager=AsyncMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(rate_limit, rate_limit, rate_limit),
        message_adapter=Mock(),
    )
    await mq._refill_rate_limits()
    assert rate_limit.refill.await_count == 3


@pytest.mark.asyncio
async def test_set_gateway_adapter(message_queue):
    adapter = Mock()
    message_queue.set_gateway_message_adapter(adapter)
    assert message_queue._gateway_adapter is adapter


@pytest.mark.asyncio
async def test_wait_for_message_cancel():
    stop = asyncio.Event()
    stop.set()
    mq = MessageQueue(
        mqtt_manager=AsyncMock(),
        main_stop_event=stop,
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=Mock(),
    )

    await mq._queue.put("dummy")

    result = await mq._wait_for_message()
    assert result == "dummy"


@pytest.mark.asyncio
async def test_clear_futures_result_set():
    fut = asyncio.Future()
    fut.uuid = "test-future"
    msg = b"data"
    fut.set_result = Mock()
    mq = MessageQueue(
        mqtt_manager=AsyncMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=Mock(),
    )
    mq._queue.put_nowait(MqttPublishMessage("topic", msg, qos=1, delivery_futures=[fut]))
    mq.clear()
    assert mq.is_empty()


@pytest.mark.asyncio
async def test_message_queue_batching_respects_type_and_size():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    stop_event = asyncio.Event()
    message_adapter = JsonMessageAdapter()

    mq = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=stop_event,
        device_rate_limiter=RateLimiter(RateLimit('0:0,'), RateLimit('0:0,'), RateLimit('0:0,')),
        message_adapter=message_adapter,
        batch_collect_max_time_ms=200,
        max_queue_size=100,
    )
    mq._backpressure.should_pause.return_value = False
    mq._mqtt_manager.backpressure.should_pause.return_value = False

    builder1 = DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("key1", 1))
    msg1 = builder1.build()

    builder2 = DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("key2", 2))
    msg2 = builder2.build()

    # First message fits, second exceeds total max_payload_size
    mq._queue.put_nowait(MqttPublishMessage("topic", msg1, qos=1))
    mq._queue.put_nowait(MqttPublishMessage("topic", msg2, qos=1))

    await asyncio.sleep(0.2)  # Give loop time to pick up

    await mq.shutdown()

    mqtt_manager.publish.assert_called_once()


@pytest.mark.asyncio
async def test_schedule_delayed_retry_reenqueues_message():
    msg = MqttPublishMessage(topic="test/topic", payload=b"data", qos=1)

    queue = MessageQueue(
        mqtt_manager=MagicMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=MagicMock()
    )

    queue._active.set()
    queue._main_stop_event.clear()

    queue._queue = asyncio.Queue()
    queue._retry_tasks.clear()

    queue._schedule_delayed_retry(msg, delay=0.05)

    await asyncio.sleep(0.1)

    requeued_msg = await queue._queue.get()
    assert requeued_msg.topic == msg.topic

    assert queue._wakeup_event.is_set()

    await asyncio.sleep(0)

    assert len(queue._retry_tasks) == 0


@pytest.mark.asyncio
async def test_schedule_delayed_retry_does_nothing_if_inactive():
    msg = MqttPublishMessage(topic="inactive/topic", payload=b"data", qos=1)

    queue = MessageQueue(
        mqtt_manager=MagicMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=MagicMock()
    )

    queue._active.clear()
    queue._main_stop_event.clear()

    queue._queue = asyncio.Queue()
    queue._retry_tasks.clear()

    queue._schedule_delayed_retry(msg, delay=0.01)

    await asyncio.sleep(0.05)

    assert queue._queue.empty()
    assert len(queue._retry_tasks) == 0


@pytest.mark.asyncio
async def test_schedule_delayed_retry_does_nothing_if_stopped():
    msg = MqttPublishMessage(topic="stop/topic", payload=b"data", qos=1)

    stop_event = asyncio.Event()
    stop_event.set()

    queue = MessageQueue(
        mqtt_manager=MagicMock(),
        main_stop_event=stop_event,
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=MagicMock()
    )

    queue._active.set()
    queue._queue = asyncio.Queue()
    queue._retry_tasks.clear()

    queue._schedule_delayed_retry(msg, delay=0.01)

    await asyncio.sleep(0.05)

    assert queue._queue.empty()
    assert len(queue._retry_tasks) == 0


@pytest.mark.asyncio
async def test_try_publish_telemetry_rate_limit_triggered():
    telemetry_limit = Mock(spec=RateLimit)
    telemetry_limit.try_consume = AsyncMock(return_value=(10, 1))
    telemetry_limit.minimal_timeout = 0.05

    mq = MessageQueue(
        mqtt_manager=AsyncMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(MagicMock(), telemetry_limit, MagicMock()),
        message_adapter=Mock(build_uplink_messages=Mock(return_value=[]))
    )
    mq._schedule_delayed_retry = AsyncMock()
    mq._backpressure = Mock(spec=BackpressureController)
    mq._backpressure.should_pause.return_value = False

    message = MqttPublishMessage(topic=DEVICE_TELEMETRY_TOPIC, payload=b"{}", qos=1)

    await mq._try_publish(message)

    await mq.shutdown()

    mq._schedule_delayed_retry.assert_called_once_with(message, delay=telemetry_limit.minimal_timeout)
    called_args = mq._schedule_delayed_retry.call_args
    assert called_args.args[0] == message


@pytest.mark.asyncio
async def test_try_publish_telemetry_dp_rate_limit_triggered():
    dp_limit = Mock(spec=RateLimit)
    dp_limit.try_consume = AsyncMock(return_value=(100, 10))
    dp_limit.minimal_timeout = 0.1

    mq = MessageQueue(
        mqtt_manager=AsyncMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(RateLimit('0:0,'), RateLimit('0:0,'), dp_limit),
        message_adapter=Mock(build_uplink_messages=Mock(return_value=[]))
    )
    mq._schedule_delayed_retry = AsyncMock()
    mq._backpressure = Mock(spec=BackpressureController)
    mq._backpressure.should_pause.return_value = False

    message = MqttPublishMessage(
        topic=DEVICE_TELEMETRY_TOPIC,
        payload=b"{}",
        qos=1,
        datapoints=5
    )

    await mq._try_publish(message)

    mq._schedule_delayed_retry.assert_called_once_with(message, delay=dp_limit.minimal_timeout)


@pytest.mark.asyncio
async def test_try_publish_generic_message_rate_limit_triggered():
    generic_limit = Mock(spec=RateLimit)
    generic_limit.try_consume = AsyncMock(return_value=(1, 60))
    generic_limit.minimal_timeout = 0.2

    mq = MessageQueue(
        mqtt_manager=AsyncMock(),
        main_stop_event=asyncio.Event(),
        device_rate_limiter=RateLimiter(generic_limit, MagicMock(), MagicMock()),
        message_adapter=Mock()
    )
    mq._schedule_delayed_retry = AsyncMock()
    mq._backpressure = Mock(spec=BackpressureController)
    mq._backpressure.should_pause.return_value = False

    message = MqttPublishMessage(topic="some/other/topic", payload=b"{}", qos=1)

    await mq._try_publish(message)

    mq._schedule_delayed_retry.assert_called_once_with(message, delay=generic_limit.minimal_timeout)
    called_args = mq._schedule_delayed_retry.call_args
    assert called_args.args[0] == message


from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_batch_breaks_on_elapsed_time():
    msg = MqttPublishMessage(
        "topic",
        DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("k", 1)).build(),
        qos=1
    )

    adapter = JsonMessageAdapter()
    adapter.splitter.max_payload_size = 1000000

    stop = asyncio.Event()
    mqtt_manager = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    mq = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=stop,
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=adapter,
        batch_collect_max_time_ms=1,
        batch_collect_max_count=1000
    )

    mq._try_publish = AsyncMock()

    mq._queue.put_nowait(msg)
    await asyncio.sleep(0.1)
    await mq.shutdown()

    mq._try_publish.assert_called_once()


@pytest.mark.asyncio
async def test_batch_breaks_on_message_count():
    adapter = JsonMessageAdapter()
    adapter.splitter.max_payload_size = 1000000

    mqtt_manager = AsyncMock()
    mqtt_manager.publish = AsyncMock(return_value=asyncio.Future())
    mqtt_manager.publish.return_value.set_result(True)
    mqtt_manager.connected = AsyncMock()
    mqtt_manager.connected.is_set.return_value = True
    mqtt_manager.backpressure.should_pause.return_value = False

    stop = asyncio.Event()

    mq = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=stop,
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=adapter,
        batch_collect_max_time_ms=1000,
        batch_collect_max_count=2
    )
    mq._try_publish = AsyncMock()

    msg1 = MqttPublishMessage("topic", DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("a", 1)).build(), qos=1)
    msg2 = MqttPublishMessage("topic", DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("b", 2)).build(), qos=1)
    msg3 = MqttPublishMessage("topic", DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("c", 3)).build(), qos=1)

    await mq._queue.put(msg1)
    await mq._queue.put(msg2)
    await mq._queue.put(msg3)
    await asyncio.sleep(0.2)
    await mq.shutdown()

    assert mq._try_publish.call_count == 2


@pytest.mark.asyncio
async def test_batch_breaks_on_type_mismatch():

    adapter = JsonMessageAdapter()
    adapter.splitter.max_payload_size = 1000000

    mqtt_manager = AsyncMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    stop = asyncio.Event()

    mq = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=stop,
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=adapter,
        batch_collect_max_time_ms=1000,
        batch_collect_max_count=10
    )
    mq._try_publish = AsyncMock()

    msg1 = MqttPublishMessage("topic",
                              DeviceUplinkMessageBuilder()
                              .add_timeseries(TimeseriesEntry("a", 1))
                              .build(), qos=1)
    msg2 = MqttPublishMessage("topic",
                              GatewayUplinkMessageBuilder()
                              .set_device_name("test_device")
                              .add_timeseries(TimeseriesEntry("b", 2))
                              .build(),
                              qos=1)

    mq._queue.put_nowait(msg1)
    mq._queue.put_nowait(msg2)

    await asyncio.sleep(0.2)
    await mq.shutdown()

    assert mq._try_publish.call_count == 2


@pytest.mark.asyncio
async def test_batch_breaks_on_size_threshold():
    from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder

    adapter = JsonMessageAdapter()
    adapter.splitter.max_payload_size = 20

    mqtt_manager = AsyncMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    stop = asyncio.Event()

    mq = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=stop,
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=adapter,
        batch_collect_max_time_ms=1000,
        batch_collect_max_count=10
    )
    mq._try_publish = AsyncMock()

    msg1 = MqttPublishMessage("topic", DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("a", 1)).build(), qos=1)
    msg2 = MqttPublishMessage("topic", DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("b", 2)).build(), qos=1)

    mq._queue.put_nowait(msg1)
    mq._queue.put_nowait(msg2)

    await asyncio.sleep(0.2)
    await mq.shutdown()

    assert mq._try_publish.call_count == 2


@pytest.mark.asyncio
async def test_batch_skips_bytes_payload():
    mqtt_manager = AsyncMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    stop = asyncio.Event()

    mq = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=stop,
        device_rate_limiter=RateLimiter(MagicMock(), MagicMock(), MagicMock()),
        message_adapter=JsonMessageAdapter(),
        batch_collect_max_time_ms=1000,
        batch_collect_max_count=1000
    )
    mq._try_publish = AsyncMock()


    msg1 = MqttPublishMessage("topic", DeviceUplinkMessageBuilder().add_timeseries(TimeseriesEntry("a", 1)).build(), qos=1)
    msg2 = MqttPublishMessage("topic", b"raw", qos=1)

    mq._queue.put_nowait(msg1)
    mq._queue.put_nowait(msg2)

    await asyncio.sleep(0.2)
    await mq.shutdown()
    assert mq._try_publish.call_count == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
