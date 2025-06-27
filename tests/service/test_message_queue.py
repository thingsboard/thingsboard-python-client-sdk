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
from contextlib import AsyncExitStack
from time import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.constants.service_keys import TELEMETRY_DATAPOINTS_RATE_LIMIT
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.message_dispatcher import JsonMessageDispatcher
from tb_mqtt_client.service.message_queue import MessageQueue


@pytest.mark.asyncio
async def test_batching_device_uplink_message():
    mqtt_manager = MagicMock()
    future = asyncio.Future()
    future.set_result(PublishResult(mqtt_topics.DEVICE_TELEMETRY_TOPIC, 1, 0, 7, 0))
    mqtt_manager.publish = AsyncMock(return_value=future)
    mqtt_manager.backpressure.should_pause.return_value = False
    main_stop_event = asyncio.Event()

    delivery_future = asyncio.Future()
    dummy_message = MagicMock()
    dummy_message.size = 10
    dummy_message.device_name = "device"
    dummy_message.get_delivery_futures.return_value = [delivery_future]

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100
    dispatcher.build_uplink_payloads.return_value = [
        (mqtt_topics.DEVICE_TELEMETRY_TOPIC, b'batch_payload', 1, [delivery_future])
    ]

    queue = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=main_stop_event,
        message_rate_limit=None,
        telemetry_rate_limit=None,
        telemetry_dp_rate_limit=None,
        message_dispatcher=dispatcher,
        batch_collect_max_time_ms=50,
        batch_collect_max_count=10
    )

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, dummy_message, 1, qos=1)
    await asyncio.sleep(0.1)
    await queue.shutdown()

    assert delivery_future.done()
    assert isinstance(delivery_future.result(), PublishResult)


@pytest.mark.asyncio
async def test_telemetry_rate_limit_retry_triggered():
    telemetry_limit = MagicMock()
    telemetry_limit.try_consume = AsyncMock(return_value=(10, 1))
    telemetry_limit.minimal_timeout = 0.01
    telemetry_limit.to_dict.return_value = {"limit": "mock"}

    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    main_stop_event = asyncio.Event()

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000

    delivery_future = asyncio.Future()
    delivery_future.set_result(PublishResult(mqtt_topics.DEVICE_TELEMETRY_TOPIC, 1, 0, 5, 0))
    dispatcher.build_uplink_payloads.return_value = [
        (mqtt_topics.DEVICE_TELEMETRY_TOPIC, b'dummy_payload', 1, [delivery_future])
    ]

    msg = DeviceUplinkMessageBuilder()
    msg.set_device_name("device")
    msg.add_timeseries(TimeseriesEntry("temp", 1))
    msg = msg.build()

    queue = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=main_stop_event,
        message_rate_limit=None,
        telemetry_rate_limit=telemetry_limit,
        telemetry_dp_rate_limit=None,
        message_dispatcher=dispatcher
    )

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg, 1, qos=1)
    await asyncio.sleep(0.1)
    await queue.shutdown()

    telemetry_limit.try_consume.assert_awaited()


@pytest.mark.asyncio
async def test_shutdown_clears_queue():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []
    stop_event = asyncio.Event()

    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)
    dummy = MagicMock()
    dummy.size = 1
    dummy.get_delivery_futures.return_value = []
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, dummy, 1, 1)
    await queue.shutdown()
    assert queue.is_empty()


@pytest.mark.asyncio
async def test_publish_raw_bytes_success():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []
    main_stop_event = asyncio.Event()

    queue = MessageQueue(mqtt_manager, main_stop_event, None, None, None, dispatcher)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"payload", 1, qos=1)
    await asyncio.sleep(0.05)
    await queue.shutdown()
    mqtt_manager.publish.assert_called()


@pytest.mark.asyncio
async def test_publish_device_uplink_message_batched():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    main_stop_event = asyncio.Event()

    future = asyncio.Future()
    dummy_msg = MagicMock()
    dummy_msg.size = 10
    dummy_msg.device_name = "dev"
    dummy_msg.get_delivery_futures.return_value = [future]

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100
    dispatcher.build_uplink_payloads.return_value = [
        (mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"batch", 1, [future])
    ]

    queue = MessageQueue(mqtt_manager, main_stop_event, None, None, None, dispatcher)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, dummy_msg, 1, qos=1)
    await asyncio.sleep(0.1)
    await queue.shutdown()
    assert future.done()


@pytest.mark.asyncio
async def test_rate_limit_telemetry_triggers_retry():
    limit = MagicMock()
    limit.try_consume = AsyncMock(return_value=(1, 1))
    limit.minimal_timeout = 0.01
    limit.to_dict.return_value = {}

    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []
    main_stop_event = asyncio.Event()

    msg = MagicMock()
    msg.device_name = "d"
    msg.size = 1
    msg.get_delivery_futures.return_value = []

    queue = MessageQueue(mqtt_manager, main_stop_event, None, limit, None, dispatcher)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg, 1, 1)
    await asyncio.sleep(0.2)
    await queue.shutdown()
    mqtt_manager.publish.assert_not_called()


@pytest.mark.asyncio
async def test_retry_on_exception():
    mqtt_manager = MagicMock()
    publish_mock = AsyncMock()
    first_attempt = RuntimeError("fail")

    second_attempt_future = asyncio.Future()

    async def complete_publish_result_later():
        await asyncio.sleep(0.1)
        publish_result = PublishResult(mqtt_topics.DEVICE_TELEMETRY_TOPIC, 1, 0, 1, 0)
        second_attempt_future.set_result(publish_result)

    asyncio.create_task(complete_publish_result_later())

    publish_mock.side_effect = [first_attempt, second_attempt_future]

    mqtt_manager.publish = publish_mock
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000

    future = asyncio.Future()
    dummy_msg = MagicMock()
    dummy_msg.device_name = "dev"
    dummy_msg.size = 10
    dummy_msg.get_delivery_futures.return_value = [future]

    dispatcher.build_uplink_payloads.return_value = [
        (mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"payload", 1, [future])
    ]

    stop_event = asyncio.Event()
    queue = MessageQueue(
        mqtt_manager,
        stop_event,
        None, None, None,
        dispatcher,
        batch_collect_max_time_ms=10
    )

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, dummy_msg, 1, qos=1)

    await asyncio.sleep(0.5)
    await queue.shutdown()

    assert mqtt_manager.publish.call_count == 2
    assert future.done()
    assert isinstance(future.result(), PublishResult)


@pytest.mark.asyncio
async def test_mixed_raw_and_structured_queue():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    stop_event = asyncio.Event()

    future = asyncio.Future()
    uplink_msg = MagicMock()
    uplink_msg.device_name = "x"
    uplink_msg.size = 10
    uplink_msg.get_delivery_futures.return_value = [future]

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100
    dispatcher.build_uplink_payloads.return_value = [
        (mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"batched", 1, [future])
    ]

    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher, batch_collect_max_time_ms=20)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"raw", 1, 1)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, uplink_msg, 1, 1)
    await asyncio.sleep(0.1)
    await queue.shutdown()
    assert future.done()


@pytest.mark.asyncio
async def test_rate_limit_refill_executes():
    r1, r2, r3 = MagicMock(), MagicMock(), MagicMock()
    for r in (r1, r2, r3):
        r.refill = AsyncMock()
        r.to_dict.return_value = {}

    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []
    stop_event = asyncio.Event()

    queue = MessageQueue(mqtt_manager, stop_event, r1, r2, r3, dispatcher)
    await asyncio.sleep(1.2)
    await queue.shutdown()

    r1.refill.assert_awaited()
    r2.refill.assert_awaited()
    r3.refill.assert_awaited()


@pytest.mark.asyncio
async def test_try_publish_without_delivery_futures():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock(return_value=asyncio.Future())
    mqtt_manager.publish.return_value.set_result(PublishResult("t", 1, 1, 1, 1))
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    await queue._try_publish("custom/topic", b"payload", datapoints=1, delivery_futures_or_none=None, qos=1)
    await queue.shutdown()

    mqtt_manager.publish.assert_called_once()


@pytest.mark.asyncio
async def test_schedule_delayed_retry_skipped_if_inactive_or_stopped():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    stop_event.set()

    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)
    queue._active.clear()

    queue._schedule_delayed_retry("topic", b"data", datapoints=1, qos=1, delay=0.01)
    await queue.shutdown()


@pytest.mark.asyncio
async def test_clear_queue_sets_futures_to_publish_result():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()

    with patch.object(MessageQueue, "_dequeue_loop", new=AsyncMock()):
        queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

        dummy_msg = DeviceUplinkMessageBuilder() \
            .add_delivery_futures(asyncio.Future()) \
            .add_timeseries(TimeseriesEntry("temp", 1)) \
            .build()

        future = (await queue.publish("some/topic", dummy_msg, 1, 1))[0]

        queue.clear()

        assert future.done()
        result = future.result()
        assert isinstance(result, PublishResult)
        assert result.topic == "some/topic"
        assert result.payload_size == dummy_msg.size
        assert result.reason_code == -1
        assert result.qos == 1
        assert result.message_id == -1

        await queue.shutdown()


@pytest.mark.asyncio
async def test_wait_for_message_exit_on_inactive():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)
    queue._active.clear()

    with pytest.raises(asyncio.CancelledError):
        await queue._wait_for_message()
    await queue.shutdown()


@pytest.mark.asyncio
async def test_schedule_delayed_retry_requeues_message():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()

    with patch("tb_mqtt_client.service.message_queue.MessageQueue._dequeue_loop", new=AsyncMock()):
        queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

        future = asyncio.Future()
        dummy_msg = MagicMock()
        dummy_msg.device_name = "dev"
        dummy_msg.size = 10
        dummy_msg.get_delivery_futures.return_value = [future]

        queue._schedule_delayed_retry(
            topic="retry/topic",
            payload=b"retry-payload",
            datapoints=1,
            qos=1,
            delay=0.05,
            delivery_futures=[future]
        )

        await asyncio.sleep(0.1)
        assert not queue.is_empty()

        await queue.shutdown()


@pytest.mark.asyncio
async def test_cancel_tasks_clears_all():
    mqtt_manager = MagicMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    async def dummy():
        await asyncio.sleep(1)

    task = asyncio.create_task(dummy())
    queue._retry_tasks.add(task)

    await queue._cancel_tasks(queue._retry_tasks)
    assert len(queue._retry_tasks) == 0


@pytest.mark.asyncio
async def test_clear_queue_with_bytes_message():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000

    stop_event = asyncio.Event()
    with patch.object(MessageQueue, "_dequeue_loop", new=AsyncMock()):
        queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

        future = asyncio.Future()
        await queue.publish("raw/topic", b"abc", 1, 0)
        queue._queue._queue[0] = ("raw/topic", b"abc", [future], 1, 0)

        queue.clear()
        assert future.done()
        result = future.result()
        assert result.topic == "raw/topic"
        assert result.payload_size == 3
        assert result.qos == 0
        assert result.reason_code == -1
        assert result.message_id == -1

    await queue.shutdown()


@pytest.mark.asyncio
async def test_resolve_attached_handles_publish_exception():
    future = asyncio.Future()
    future.set_exception(RuntimeError("fail"))

    f1 = asyncio.Future()
    f2 = asyncio.Future()

    dummy_payload = b"abc"
    topic = "topic"
    qos = 1

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock(return_value=future)

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    await queue._try_publish(
        topic=topic,
        payload=dummy_payload,
        datapoints=1,
        delivery_futures_or_none=[f1, f2],
        qos=qos
    )

    await asyncio.sleep(0.05)
    assert f1.done() and f2.done()
    for f in (f1, f2):
        res = f.result()
        assert isinstance(res, PublishResult)
        assert res.reason_code == -1

    await queue.shutdown()


@pytest.mark.asyncio
async def test_try_publish_message_type_non_telemetry():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock()

    rate_limit = MagicMock()
    rate_limit.try_consume = AsyncMock(return_value=None)
    rate_limit.to_dict.return_value = {}
    rate_limit.minimal_timeout = 0.1

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, message_rate_limit=rate_limit,
                         telemetry_rate_limit=None, telemetry_dp_rate_limit=None,
                         message_dispatcher=dispatcher)

    await queue._try_publish(
        topic="non/telemetry",
        payload=b"x",
        datapoints=1,
        delivery_futures_or_none=[],
        qos=0
    )
    mqtt_manager.publish.assert_called_once()
    await queue.shutdown()


@pytest.mark.asyncio
async def test_shutdown_rate_limit_task_cancel_only():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager.publish = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    # Cancel only the rate limit task before shutdown
    queue._rate_limit_refill_task.cancel()

    await queue.shutdown()
    assert queue._rate_limit_refill_task.cancelled()


@pytest.mark.asyncio
async def test_schedule_delayed_retry_when_main_stop_active():
    mqtt_manager = MagicMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000

    stop_event = asyncio.Event()
    stop_event.set()

    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    queue._active.clear()

    queue._schedule_delayed_retry("x", b"y", 1, 0, 0.01)
    await asyncio.sleep(0.05)
    assert queue._queue.empty()
    await queue.shutdown()


@pytest.mark.asyncio
async def test_publish_queue_full_sets_failed_result_for_bytes():
    mqtt_manager = MagicMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    stop_event = asyncio.Event()

    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher, max_queue_size=1)
    await queue.publish("t", b"raw", 1, qos=0)

    queue._queue.put_nowait = MagicMock(side_effect=asyncio.QueueFull)

    result = await queue.publish("t", b"raw", 1, qos=0)
    assert result is not None
    assert isinstance(result[0], asyncio.Future)
    await asyncio.sleep(0)
    assert result[0].done()
    assert result[0].result().reason_code == -1

    await queue.shutdown()


@pytest.mark.asyncio
async def test_wait_for_message_raises_cancelled():
    mqtt_manager = MagicMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    queue._active.clear()

    with pytest.raises(asyncio.CancelledError):
        await queue._wait_for_message()

    await queue.shutdown()


@pytest.mark.asyncio
async def test_batch_loop_breaks_on_count_threshold():
    # Setup: fake PublishResult to return
    publish_result = PublishResult(
        topic=mqtt_topics.DEVICE_TELEMETRY_TOPIC,
        qos=1,
        message_id=42,
        payload_size=8,
        reason_code=0
    )

    # This is what the MQTT manager's publish will return
    publish_future = asyncio.Future()
    publish_future.set_result(publish_result)

    # Now mock the MQTT manager
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock(return_value=publish_future)
    mqtt_manager.backpressure.should_pause.return_value = False

    # This is the future that the message queue should resolve
    delivery_future = asyncio.Future()

    # Mock dispatcher to output the delivery future
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = [
        (mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"batched", 1, [delivery_future])
    ]

    # Create and start the queue
    stop_event = asyncio.Event()
    queue = MessageQueue(
        mqtt_manager, stop_event,
        message_rate_limit=None,
        telemetry_rate_limit=None,
        telemetry_dp_rate_limit=None,
        message_dispatcher=dispatcher,
        batch_collect_max_count=2
    )

    dummy_msg = MagicMock()
    dummy_msg.size = 10
    dummy_msg.device_name = "dev"
    dummy_msg.get_delivery_futures.return_value = [delivery_future]

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, dummy_msg, 1, 1)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, dummy_msg, 1, 1)

    # Allow time for batching and publishing
    await asyncio.sleep(0.1)
    await queue.shutdown()

    assert mqtt_manager.publish.called
    assert delivery_future.done()
    result = delivery_future.result()
    assert isinstance(result, PublishResult)
    assert result.topic == mqtt_topics.DEVICE_TELEMETRY_TOPIC


@pytest.mark.asyncio
async def test_batch_loop_skips_message_on_size_exceed():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 15
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    small_msg = MagicMock()
    small_msg.size = 10
    small_msg.device_name = "dev"
    small_msg.get_delivery_futures.return_value = []

    large_msg = MagicMock()
    large_msg.size = 10
    large_msg.device_name = "dev"
    large_msg.get_delivery_futures.return_value = []

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, small_msg, 1, 1)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, large_msg, 1, 1)

    await asyncio.sleep(0.1)
    await queue.shutdown()

    assert mqtt_manager.publish.called


@pytest.mark.asyncio
async def test_batch_requeues_on_size_exceed():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 15
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    msg1 = MagicMock()
    msg1.size = 10
    msg1.device_name = "dev"
    msg1.get_delivery_futures.return_value = []

    msg2 = MagicMock()
    msg2.size = 10
    msg2.device_name = "dev"
    msg2.get_delivery_futures.return_value = []

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg1, 1, 1)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg2, 1, 1)

    await asyncio.sleep(0.1)
    await queue.shutdown()

    assert mqtt_manager.publish.call_count >= 1


@pytest.mark.asyncio
async def test_batch_immediate_publish_on_raw_bytes():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"raw_payload", 1, 1)

    await asyncio.sleep(0.05)
    await queue.shutdown()

    mqtt_manager.publish.assert_called()
    args, kwargs = mqtt_manager.publish.call_args
    assert isinstance(kwargs['payload'], bytes)


@pytest.mark.asyncio
async def test_batch_queue_empty_breaks_safely():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)

    await asyncio.sleep(0.05)
    await queue.shutdown()

    assert mqtt_manager.publish.call_count == 0


@pytest.mark.asyncio
async def test_try_publish_telemetry_rate_limited():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 1000
    dispatcher.build_uplink_payloads.return_value = [("topic", b"{}", 3, [])]
    telemetry_rate_limit = MagicMock()
    telemetry_rate_limit.try_consume = AsyncMock(return_value=(10, 1))
    telemetry_rate_limit.minimal_timeout = 0.5

    stop_event = asyncio.Event()
    queue = MessageQueue(
        mqtt_manager,
        stop_event,
        telemetry_rate_limit,
        None,
        RateLimit("10:1", TELEMETRY_DATAPOINTS_RATE_LIMIT, 100),
        dispatcher
    )

    queue._schedule_delayed_retry = MagicMock()

    msg = (DeviceUplinkMessageBuilder().add_timeseries(
        [TimeseriesEntry("temp", 1),
         TimeseriesEntry("hum", 2),
         TimeseriesEntry("pres", 3)])
           .build())

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg, msg.timeseries_datapoint_count(), 1)
    await asyncio.sleep(0.1)
    await queue.shutdown()

    queue._schedule_delayed_retry.assert_called_once()
    mqtt_manager.publish.assert_not_called()


@pytest.mark.asyncio
async def test_try_publish_non_telemetry_rate_limited():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 1000
    dispatcher.build_uplink_payloads.return_value = [("topic", b"{}", 1)]

    message_rate_limit = MagicMock()
    message_rate_limit.try_consume = AsyncMock(return_value=(5, 60))
    message_rate_limit.minimal_timeout = 1.0

    stop_event = asyncio.Event()
    queue = MessageQueue(
        mqtt_manager,
        stop_event,
        telemetry_rate_limit=None,
        message_rate_limit=message_rate_limit,
        telemetry_dp_rate_limit=None,
        message_dispatcher=dispatcher
    )

    queue._schedule_delayed_retry = MagicMock()

    payload = b'raw-bytes'
    topic = "v1/devices/me/rpc/request/1"

    await queue._try_publish(topic, payload, datapoints=0, qos=1, delivery_futures_or_none=None)
    await asyncio.sleep(0.05)
    await queue.shutdown()

    message_rate_limit.try_consume.assert_awaited_once_with(1)
    queue._schedule_delayed_retry.assert_called_once_with(
        topic=topic,
        payload=payload,
        datapoints=0,
        qos=1,
        delay=1.0,
        delivery_futures=[]
    )
    mqtt_manager.publish.assert_not_called()


@pytest.mark.parametrize("paused", [True, False])
@pytest.mark.asyncio
async def test_backpressure_delays_publish(paused, monkeypatch):
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = paused

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()
    queue = MessageQueue(
        mqtt_manager,
        stop_event,
        message_rate_limit=None,
        telemetry_rate_limit=None,
        telemetry_dp_rate_limit=None,
        message_dispatcher=dispatcher,
        batch_collect_max_count=1
    )

    scheduled_retry_mock = MagicMock()
    monkeypatch.setattr(queue, "_schedule_delayed_retry", scheduled_retry_mock)

    await queue._try_publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, b"test_payload", 1, qos=1)
    await asyncio.sleep(0.05)

    if paused:
        scheduled_retry_mock.assert_called_once_with(
            topic=mqtt_topics.DEVICE_TELEMETRY_TOPIC,
            payload=b"test_payload",
            datapoints=1,
            qos=1,
            delay=1.0,
            delivery_futures=[]
        )
        mqtt_manager.publish.assert_not_called()
    else:
        scheduled_retry_mock.assert_not_called()
        mqtt_manager.publish.assert_called_once_with(
            message_or_topic=mqtt_topics.DEVICE_TELEMETRY_TOPIC,
            payload=b"test_payload",
            qos=1
        )

    await queue.shutdown()


@pytest.mark.asyncio
async def test_publish_telemetry_rate_limit_triggered():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    stop_event = asyncio.Event()

    telemetry_dp_rate_limit = MagicMock()
    telemetry_dp_rate_limit.try_consume = AsyncMock(return_value=(10, 60))
    telemetry_dp_rate_limit.minimal_timeout = 1.23

    async with AsyncExitStack() as stack:
        queue = MessageQueue(
            mqtt_manager,
            stop_event,
            None,
            None,
            telemetry_dp_rate_limit=telemetry_dp_rate_limit,
            message_dispatcher=dispatcher,
        )
        stack.push_async_callback(queue.shutdown)

        msg = DeviceUplinkMessageBuilder() \
            .add_timeseries([TimeseriesEntry(f"temp{i}", i) for i in range(10)]) \
            .build()

        with patch.object(queue, "_schedule_delayed_retry", wraps=queue._schedule_delayed_retry) as delayed_retry_mock:
            await queue._try_publish(
                mqtt_topics.DEVICE_TELEMETRY_TOPIC,
                b'',
                qos=1,
                datapoints=msg.timeseries_datapoint_count()
            )

            mqtt_manager.publish.assert_not_called()
            delayed_retry_mock.assert_called_once()
            args, kwargs = delayed_retry_mock.call_args
            assert kwargs["topic"] == mqtt_topics.DEVICE_TELEMETRY_TOPIC
            assert kwargs["delay"] == telemetry_dp_rate_limit.minimal_timeout


@pytest.mark.asyncio
async def test_batch_loop_large_messages_are_split_and_published():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = JsonMessageDispatcher(100, 20)

    stop_event = asyncio.Event()
    queue = MessageQueue(
        mqtt_manager,
        stop_event,
        None,
        None,
        None,
        message_dispatcher=dispatcher,
        max_queue_size=100,
        batch_collect_max_time_ms=10
    )

    builder1 = DeviceUplinkMessageBuilder()
    for i in range(30):
        builder1.add_timeseries(TimeseriesEntry(f"t{i}", i))
    message1 = builder1.build()

    builder2 = DeviceUplinkMessageBuilder()
    for i in range(30, 60):
        builder2.add_timeseries(TimeseriesEntry(f"t{i}", i))
    message2 = builder2.build()

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, message1, message1.timeseries_datapoint_count(), qos=1)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, message2, message2.timeseries_datapoint_count(), qos=1)

    await asyncio.sleep(0.2)
    await queue.shutdown()

    assert mqtt_manager.publish.call_count == 6

    for call in mqtt_manager.publish.call_args_list:
        kwargs = call.kwargs
        assert kwargs["message_or_topic"] == "v1/devices/me/telemetry"
        assert isinstance(kwargs["payload"], bytes)
        assert kwargs["qos"] == 1


@pytest.mark.asyncio
async def test_delivery_futures_resolved_via_real_puback_handler():
    delivery_future = asyncio.Future()

    mqtt_future = asyncio.Future()
    mqtt_future.mid = 123

    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock(return_value=mqtt_future)
    mqtt_manager.backpressure.should_pause.return_value = False
    mqtt_manager._backpressure = MagicMock()
    mqtt_manager._on_publish_result_callback = None

    from tb_mqtt_client.service.mqtt_manager import MQTTManager
    mqtt_manager._handle_puback_reason_code = MQTTManager._handle_puback_reason_code.__get__(mqtt_manager)

    topic = mqtt_topics.DEVICE_TELEMETRY_TOPIC
    qos = 1
    payload_size = 24
    publish_time = 1.0
    mqtt_manager._pending_publishes = {
        mqtt_future.mid: (delivery_future, topic, qos, payload_size, publish_time)
    }

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = [
        (topic, b'{"some":"payload"}', qos, [delivery_future])
    ]

    stop_event = asyncio.Event()
    queue = MessageQueue(
        mqtt_manager,
        stop_event,
        None,
        None,
        None,
        message_dispatcher=dispatcher,
        batch_collect_max_count=1,
        batch_collect_max_time_ms=1
    )

    msg = (
        DeviceUplinkMessageBuilder()
        .set_device_name("deviceA")
        .add_timeseries([TimeseriesEntry("temperature", 25)])
        .add_delivery_futures([delivery_future])
        .build()
    )

    await queue.publish(topic, msg, msg.timeseries_datapoint_count(), qos=qos)
    await asyncio.sleep(0.05)

    mqtt_future.set_result(None)
    mqtt_manager._handle_puback_reason_code(mqtt_future.mid, 0, {})

    await asyncio.sleep(0.05)

    assert delivery_future.done()
    result = delivery_future.result()
    assert isinstance(result, PublishResult)
    assert result.topic == topic
    assert result.message_id == mqtt_future.mid
    assert result.reason_code == 0


@pytest.mark.asyncio
async def test_batch_append_and_batch_size_accumulate():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False

    dispatcher = JsonMessageDispatcher(100000, 10000)
    stop_event = asyncio.Event()

    queue = MessageQueue(
        mqtt_manager,
        stop_event,
        None,
        None,
        None,
        message_dispatcher=dispatcher,
        batch_collect_max_count=2,
        batch_collect_max_time_ms=1000
    )

    fixed_ts = int(time() * 1000)

    msg1 = DeviceUplinkMessageBuilder() \
        .add_timeseries([TimeseriesEntry(f"temp{i}", i, ts=fixed_ts) for i in range(10)]) \
        .build()
    msg2 = DeviceUplinkMessageBuilder() \
        .add_timeseries([TimeseriesEntry(f"temp{i}", i, ts=fixed_ts) for i in range(10)]) \
        .build()

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg1, msg1.timeseries_datapoint_count(), 1)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg2, msg2.timeseries_datapoint_count(), 1)

    await asyncio.sleep(0.3)
    await queue.shutdown()

    mqtt_manager.publish.assert_called_once()

    args, kwargs = mqtt_manager.publish.call_args
    assert kwargs["message_or_topic"] == mqtt_topics.DEVICE_TELEMETRY_TOPIC
    assert isinstance(kwargs["payload"], bytes)
    assert kwargs["qos"] == 1
