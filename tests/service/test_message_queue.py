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
from unittest.mock import AsyncMock, MagicMock

from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.message_queue import MessageQueue


@pytest.mark.asyncio
async def test_publish_raw_bytes_success():
    mqtt_manager = MagicMock()
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    main_stop_event = asyncio.Event()

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    queue = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=main_stop_event,
        message_rate_limit=None,
        telemetry_rate_limit=None,
        telemetry_dp_rate_limit=None,
        message_dispatcher=dispatcher,
    )

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, b'test_payload', 1, qos=1)
    await asyncio.sleep(0.05)
    await queue.shutdown()

    mqtt_manager.publish.assert_called_once()


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
    msg.add_telemetry(TimeseriesEntry("temp", 1))
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
    mqtt_manager.publish = AsyncMock()
    mqtt_manager.backpressure.should_pause.return_value = False
    main_stop_event = asyncio.Event()

    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []

    dummy_message = MagicMock()
    dummy_message.device_name = "device"
    dummy_message.size = 1
    dummy_message.get_delivery_futures.return_value = []

    queue = MessageQueue(
        mqtt_manager=mqtt_manager,
        main_stop_event=main_stop_event,
        message_rate_limit=None,
        telemetry_rate_limit=None,
        telemetry_dp_rate_limit=None,
        message_dispatcher=dispatcher
    )

    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, dummy_message, 1, qos=1)
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
async def test_backpressure_triggers_retry():
    mqtt_manager = MagicMock()
    mqtt_manager.backpressure.should_pause.return_value = True
    mqtt_manager.publish = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.splitter.max_payload_size = 100000
    dispatcher.build_uplink_payloads.return_value = []
    stop_event = asyncio.Event()

    msg = MagicMock()
    msg.size = 1
    msg.device_name = "dev"
    msg.get_delivery_futures.return_value = []

    queue = MessageQueue(mqtt_manager, stop_event, None, None, None, dispatcher)
    await queue.publish(mqtt_topics.DEVICE_TELEMETRY_TOPIC, msg, 1, 1)
    await asyncio.sleep(0.1)
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


if __name__ == '__main__':
    pytest.main([__file__])
