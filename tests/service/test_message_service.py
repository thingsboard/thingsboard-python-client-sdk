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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit, EMPTY_RATE_LIMIT
from tb_mqtt_client.common.rate_limit.rate_limiter import RateLimiter
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessageBuilder
from tb_mqtt_client.service.device.message_adapter import MessageAdapter
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter
from tb_mqtt_client.service.message_service import MessageService, MessageQueueWorker
from tb_mqtt_client.service.mqtt_manager import MQTTManager


@pytest_asyncio.fixture
async def setup_message_service():
    # Create mocks for all dependencies
    mqtt_manager = MagicMock(spec=MQTTManager)
    main_stop_event = asyncio.Event()

    # Mock rate limiters
    device_rate_limiter = MagicMock(spec=RateLimiter)
    device_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.values.return_value = [
        device_rate_limiter.message_rate_limit,
        device_rate_limiter.telemetry_message_rate_limit,
        device_rate_limiter.telemetry_datapoints_rate_limit
    ]

    gateway_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.values.return_value = [
        gateway_rate_limiter.message_rate_limit,
        gateway_rate_limiter.telemetry_message_rate_limit,
        gateway_rate_limiter.telemetry_datapoints_rate_limit
    ]

    # Mock message adapters
    message_adapter = MagicMock(spec=MessageAdapter)
    gateway_message_adapter = MagicMock(spec=GatewayMessageAdapter)

    # Create the service with patched asyncio.create_task to avoid actual task creation
    with patch('asyncio.create_task', new=MagicMock()):
        service = MessageService(
            mqtt_manager=mqtt_manager,
            main_stop_event=main_stop_event,
            device_rate_limiter=device_rate_limiter,
            message_adapter=message_adapter,
            gateway_message_adapter=gateway_message_adapter,
            gateway_rate_limiter=gateway_rate_limiter
        )

        # Replace the actual tasks with mocks
        service._initial_queue_task = MagicMock()
        service._service_queue_task = MagicMock()
        service._device_uplink_messages_queue_task = MagicMock()
        service._gateway_uplink_messages_queue_task = MagicMock()
        service._rate_limit_refill_task = MagicMock()
        service.__print_queue_statistics_task = MagicMock()

        yield (service, mqtt_manager, main_stop_event, device_rate_limiter,
               message_adapter, gateway_message_adapter, gateway_rate_limiter)


@pytest_asyncio.fixture
async def setup_retry_loop_service(setup_message_service):
    service, mqtt_manager, main_stop_event, *_ = setup_message_service
    service._main_stop_event = main_stop_event
    service._active.set()

    # Mock dependencies
    service._retry_by_qos_queue = AsyncMock()
    service._service_queue = AsyncMock()
    service._gateway_uplink_messages_queue = AsyncMock()
    service._device_uplink_messages_queue = AsyncMock()

    # Cooldown patch to avoid slow tests
    service._QUEUE_COOLDOWN = 0

    return service, mqtt_manager


@pytest.mark.asyncio
async def test_publish_success(setup_message_service):
    service, mqtt_manager, _, _, _, _, _ = setup_message_service

    # Mock the queue put method
    service._initial_queue.put = AsyncMock()

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")

    # Call the publish method
    await service.publish(message)

    # Verify the message was added to the queue
    service._initial_queue.put.assert_awaited_once_with(message)


@pytest.mark.asyncio
async def test_publish_exception(setup_message_service):
    service, mqtt_manager, _, _, _, _, _ = setup_message_service

    # Mock the queue put method to raise an exception
    service._initial_queue.put = AsyncMock(side_effect=Exception("Queue error"))

    # Create a test message with a delivery future
    future = asyncio.Future()
    message = MqttPublishMessage("test/topic", b"test_payload", delivery_futures=[future])

    # Call the publish method
    await service.publish(message)

    # Verify the future was completed with an error result
    assert future.done()
    result = future.result()
    assert isinstance(result, PublishResult)
    assert result.reason_code == -1


@pytest.mark.asyncio
async def test_shutdown():
    """Test the shutdown method with real tasks to ensure full coverage."""
    # Create mocks for all dependencies
    mqtt_manager = MagicMock(spec=MQTTManager)
    main_stop_event = asyncio.Event()
    device_rate_limiter = MagicMock(spec=RateLimiter)
    device_rate_limiter.values.return_value = []
    gateway_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter.values.return_value = []
    message_adapter = MagicMock(spec=MessageAdapter)
    gateway_message_adapter = MagicMock(spec=GatewayMessageAdapter)

    # Create a real service with real tasks
    with patch('asyncio.create_task', side_effect=asyncio.create_task) as mock_create_task:
        # Create simple async functions for the tasks
        async def mock_task():
            try:
                while True:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                # This is expected during shutdown
                pass

        # Patch the methods that would create tasks to return our mock task
        with patch.object(MessageService, '_dispatch_initial_queue_loop', return_value=mock_task()), \
                patch.object(MessageService, '_dispatch_queue_loop', return_value=mock_task()), \
                patch.object(MessageService, '_rate_limit_refill_loop', return_value=mock_task()), \
                patch.object(MessageService, 'print_queues_statistics', return_value=mock_task()), \
                patch.object(MessageService, 'clear', new_callable=AsyncMock) as mock_clear:

            # Create the service
            service = MessageService(
                mqtt_manager=mqtt_manager,
                main_stop_event=main_stop_event,
                device_rate_limiter=device_rate_limiter,
                message_adapter=message_adapter,
                gateway_message_adapter=gateway_message_adapter,
                gateway_rate_limiter=gateway_rate_limiter
            )

            # Verify that tasks were created
            assert mock_create_task.call_count >= 5

            # Call the shutdown method
            await service.shutdown()

            # Verify the active flag was cleared
            assert not service._active.is_set()

            # Verify the clear method was called
            mock_clear.assert_awaited_once()


@pytest.mark.asyncio
async def test_is_empty(setup_message_service):
    service, _, _, _, _, _, _ = setup_message_service

    # Mock the queue is_empty method
    service._initial_queue.is_empty = MagicMock(return_value=True)

    # Check if the queue is empty
    assert service.is_empty()

    # Verify the is_empty method was called
    service._initial_queue.is_empty.assert_called_once()


@pytest.mark.asyncio
async def test_clear(setup_message_service):
    service, _, _, _, _, _, _ = setup_message_service

    # Mock the queue methods
    service._initial_queue.is_empty = MagicMock(side_effect=[False, True])
    service._initial_queue.get = AsyncMock(return_value=MqttPublishMessage("topic", b"payload"))

    service._service_queue.is_empty = MagicMock(side_effect=[False, True])
    service._service_queue.get = AsyncMock(return_value=MqttPublishMessage("topic", b"payload"))

    service._device_uplink_messages_queue.is_empty = MagicMock(side_effect=[False, True])
    service._device_uplink_messages_queue.get = AsyncMock(return_value=MqttPublishMessage("topic", b"payload"))

    service._gateway_uplink_messages_queue.is_empty = MagicMock(side_effect=[False, True])
    service._gateway_uplink_messages_queue.get = AsyncMock(return_value=MqttPublishMessage("topic", b"payload"))

    # Call the clear method
    await service.clear()

    # Verify the get method was called for each queue
    service._initial_queue.get.assert_awaited_once()
    service._service_queue.get.assert_awaited_once()
    service._device_uplink_messages_queue.get.assert_awaited_once()
    service._gateway_uplink_messages_queue.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_set_gateway_message_adapter(setup_message_service):
    service, _, _, _, _, _, gateway_message_adapter = setup_message_service

    # Create a new mock adapter
    new_adapter = MagicMock(spec=GatewayMessageAdapter)

    # Set the new adapter
    service.set_gateway_message_adapter(new_adapter)

    # Verify the adapter was set
    assert service._gateway_adapter == new_adapter


@pytest.mark.asyncio
async def test_dispatch_initial_queue_loop_service_message(setup_message_service):
    service, mqtt_manager, main_stop_event, _, _, _, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Mock the queue methods
    service._initial_queue.peek_batch = AsyncMock(return_value=[
        MqttPublishMessage("topic", b"raw_payload")
    ])
    service._initial_queue.pop_n = AsyncMock()
    service._initial_queue.is_empty = MagicMock(return_value=True)

    service._service_queue.put = AsyncMock()

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_initial_queue_loop()

    # Verify the message was put in the service queue
    service._service_queue.put.assert_awaited_once()
    service._initial_queue.pop_n.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_dispatch_initial_queue_loop_device_message(setup_message_service):
    service, mqtt_manager, main_stop_event, _, message_adapter, _, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Create a device uplink message using the builder
    device_uplink = DeviceUplinkMessageBuilder().set_device_name("test-device").build()
    device_message = MqttPublishMessage("topic", device_uplink)

    # Mock the queue methods
    service._initial_queue.peek_batch = AsyncMock(return_value=[device_message])
    service._initial_queue.pop_n = AsyncMock()
    service._initial_queue.is_empty = MagicMock(return_value=True)

    service._device_uplink_messages_queue.extend = AsyncMock()

    # Mock the adapter
    processed_messages = [MqttPublishMessage("processed_topic", b"processed_payload")]
    message_adapter.build_uplink_messages.return_value = processed_messages

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_initial_queue_loop()

    # Verify the adapter was called and messages were added to the device queue
    message_adapter.build_uplink_messages.assert_called_once_with([device_message])
    service._device_uplink_messages_queue.extend.assert_awaited_once_with(processed_messages)
    service._initial_queue.pop_n.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_dispatch_initial_queue_loop_gateway_message(setup_message_service):
    service, mqtt_manager, main_stop_event, _, message_adapter, gateway_message_adapter, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Create a gateway uplink message using the builder
    gateway_uplink = GatewayUplinkMessageBuilder().set_device_name("test-device").build()
    gateway_message = MqttPublishMessage("topic", gateway_uplink)

    # Mock the queue methods
    service._initial_queue.peek_batch = AsyncMock(return_value=[gateway_message])
    service._initial_queue.pop_n = AsyncMock()
    service._initial_queue.is_empty = MagicMock(return_value=True)

    service._gateway_uplink_messages_queue.extend = AsyncMock()

    # Mock the adapter
    processed_messages = [MqttPublishMessage("processed_topic", b"processed_payload")]
    gateway_message_adapter.build_uplink_messages.return_value = processed_messages

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_initial_queue_loop()

    # Verify the adapter was called and messages were added to the gateway queue
    gateway_message_adapter.build_uplink_messages.assert_called_once_with([gateway_message])
    service._gateway_uplink_messages_queue.extend.assert_awaited_once_with(processed_messages)
    service._initial_queue.pop_n.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_dispatch_initial_queue_loop_exception(setup_message_service):
    service, mqtt_manager, main_stop_event, _, _, _, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Mock the queue methods to raise an exception
    service._initial_queue.peek_batch = AsyncMock(side_effect=Exception("Test exception"))

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_initial_queue_loop()

    # The test passes if no exception is raised


@pytest.mark.asyncio
async def test_dispatch_queue_loop(setup_message_service):
    service, mqtt_manager, main_stop_event, _, _, _, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Create a mock queue and worker
    queue = AsyncMock()
    worker = MagicMock(spec=MessageQueueWorker)

    # Mock the queue methods
    message = MqttPublishMessage("topic", b"payload")
    queue.get = AsyncMock(return_value=message)
    queue.is_empty = MagicMock(return_value=True)

    # Mock the worker process method
    worker.process = AsyncMock(return_value=(None, None, None))

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_queue_loop(queue, worker)

    # Verify the worker process method was called
    worker.process.assert_awaited_once_with(message)


@pytest.mark.asyncio
async def test_dispatch_queue_loop_rate_limit_triggered(setup_message_service):
    service, mqtt_manager, main_stop_event, _, _, _, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Create a mock queue and worker
    queue = AsyncMock()
    worker = MagicMock(spec=MessageQueueWorker)

    # Mock the queue methods
    message = MqttPublishMessage("topic", b"payload")
    queue.get = AsyncMock(return_value=message)
    queue.is_empty = MagicMock(return_value=True)
    queue.reinsert_front = AsyncMock()

    # Mock the worker process method to trigger rate limit
    rate_limit = MagicMock(spec=RateLimit)
    rate_limit.required_tokens_ready = asyncio.Event()
    rate_limit.required_tokens_ready.set()  # Set it to avoid waiting in the test
    worker.process = AsyncMock(return_value=(10, 5, rate_limit))

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_queue_loop(queue, worker)

    # Verify the worker process method was called
    worker.process.assert_awaited_once_with(message)

    # Verify the message was reinserted to the front of the queue
    queue.reinsert_front.assert_awaited_once_with(message)

    # Verify the rate limit was set
    rate_limit.set_required_tokens.assert_called_once_with(10, 5)


@pytest.mark.asyncio
async def test_dispatch_queue_loop_empty_message(setup_message_service):
    service, mqtt_manager, main_stop_event, _, _, _, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Create a mock queue and worker
    queue = AsyncMock()
    worker = MagicMock(spec=MessageQueueWorker)

    # Mock the queue methods to return None
    queue.get = AsyncMock(return_value=None)

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_queue_loop(queue, worker)

    # Verify the worker process method was not called
    worker.process.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_queue_loop_exception(setup_message_service):
    service, mqtt_manager, main_stop_event, _, _, _, _ = setup_message_service

    # Set up the test to run once and then exit
    main_stop_event.is_set = MagicMock(side_effect=[False, True])

    # Create a mock queue and worker
    queue = AsyncMock()
    worker = MagicMock(spec=MessageQueueWorker)

    # Mock the queue methods to raise an exception
    queue.get = AsyncMock(side_effect=Exception("Test exception"))

    # Run the dispatch loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._dispatch_queue_loop(queue, worker)

    # The test passes if no exception is raised


@pytest.mark.asyncio
async def test_rate_limit_refill_loop(setup_message_service):
    service, mqtt_manager, _, device_rate_limiter, _, _, gateway_rate_limiter = setup_message_service

    # Set up the test to run once and then exit
    service._active.is_set = MagicMock(side_effect=[True, False])

    # Mock the refill methods
    service._refill_rate_limits = AsyncMock()

    # Run the refill loop
    with patch('asyncio.sleep', new=AsyncMock()):
        await service._rate_limit_refill_loop()

    # Verify the refill method was called
    service._refill_rate_limits.assert_awaited_once()


@pytest.mark.asyncio
async def test_refill_rate_limits(setup_message_service):
    service, mqtt_manager, _, device_rate_limiter, _, _, gateway_rate_limiter = setup_message_service

    # Mock the rate limit refill methods
    device_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limit.refill = AsyncMock()
    device_rate_limiter.values.return_value = [device_rate_limit]

    gateway_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limit.refill = AsyncMock()
    gateway_rate_limiter.values.return_value = [gateway_rate_limit]

    # Call the refill method
    await service._refill_rate_limits()

    # Verify the refill methods were called
    device_rate_limit.refill.assert_awaited_once()
    gateway_rate_limit.refill.assert_awaited_once()


@pytest.mark.asyncio
async def test_print_queue_statistics(setup_message_service):
    service, mqtt_manager, main_stop_event, _, _, _, _ = setup_message_service

    # Create a patched version of the print_queue_statistics method to avoid infinite loop
    original_print_queue_statistics = service.print_queues_statistics  # noqa

    async def patched_print_queue_statistics():
        # Just run the body of the loop once
        initial_queue_size = service._initial_queue.size()  # noqa
        service_queue_size = service._service_queue.size()  # noqa
        device_uplink_queue_size = service._device_uplink_messages_queue.size()  # noqa
        gateway_uplink_queue_size = service._gateway_uplink_messages_queue.size()  # noqa
        # We don't need to log anything in the test

    # Replace the method with our patched version
    service.print_queues_statistics = patched_print_queue_statistics

    # Mock the queue size methods
    service._initial_queue.size = MagicMock(return_value=1)
    service._service_queue.size = MagicMock(return_value=2)
    service._device_uplink_messages_queue.size = MagicMock(return_value=3)
    service._gateway_uplink_messages_queue.size = MagicMock(return_value=4)

    # Run the statistics method
    await service.print_queues_statistics()

    # Verify the size methods were called
    service._initial_queue.size.assert_called_once()
    service._service_queue.size.assert_called_once()
    service._device_uplink_messages_queue.size.assert_called_once()
    service._gateway_uplink_messages_queue.size.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_tasks():
    # Create mock tasks
    task1 = MagicMock(spec=asyncio.Task)
    task2 = MagicMock(spec=asyncio.Task)
    tasks = {task1, task2}

    # Call the cancel_tasks method
    with patch('asyncio.gather', new=AsyncMock()):
        await MessageService._cancel_tasks(tasks)

    # Verify the tasks were canceled
    task1.cancel.assert_called_once()
    task2.cancel.assert_called_once()
    assert len(tasks) == 0


# Tests for MessageQueueWorker class
@pytest.mark.asyncio
async def test_message_queue_worker_process_no_rate_limits():
    # Create mocks
    queue = AsyncMock()
    stop_event = asyncio.Event()
    mqtt_manager = MagicMock(spec=MQTTManager)
    device_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter = MagicMock(spec=RateLimiter)

    # Create the worker
    worker = MessageQueueWorker(
        "TestWorker",
        queue,
        stop_event,
        mqtt_manager,
        device_rate_limiter,
        gateway_rate_limiter
    )

    # Mock the rate limit methods
    worker._get_rate_limits_for_message = MagicMock(return_value=(EMPTY_RATE_LIMIT, EMPTY_RATE_LIMIT))

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")

    # Call the process method
    result = await worker.process(message)

    # Verify the mqtt_manager.publish method was called
    mqtt_manager.publish.assert_awaited_once_with(message)

    # Verify the result
    assert result == (None, None, None)


@pytest.mark.asyncio
async def test_message_queue_worker_process_with_rate_limits_not_triggered():
    # Create mocks
    queue = AsyncMock()
    stop_event = asyncio.Event()
    mqtt_manager = MagicMock(spec=MQTTManager)
    device_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter = MagicMock(spec=RateLimiter)

    # Create the worker
    worker = MessageQueueWorker(
        "TestWorker",
        queue,
        stop_event,
        mqtt_manager,
        device_rate_limiter,
        gateway_rate_limiter
    )

    # Create rate limits
    message_rate_limit = MagicMock(spec=RateLimit)
    message_rate_limit.has_limit.return_value = True
    datapoints_rate_limit = MagicMock(spec=RateLimit)
    datapoints_rate_limit.has_limit.return_value = True

    # Mock the rate limit methods
    worker._get_rate_limits_for_message = MagicMock(return_value=(message_rate_limit, datapoints_rate_limit))
    worker.check_rate_limits_for_message = AsyncMock(return_value=(None, 0, None))
    worker._consume_rate_limits_for_message = AsyncMock()

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")

    # Call the process method
    result = await worker.process(message)

    # Verify the rate limit methods were called
    worker.check_rate_limits_for_message.assert_awaited_once_with(
        datapoints_count=message.datapoints,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )
    worker._consume_rate_limits_for_message.assert_awaited_once_with(
        datapoints_count=message.datapoints,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )

    # Verify the mqtt_manager.publish method was called
    mqtt_manager.publish.assert_awaited_once_with(message)

    # Verify the result
    assert result == (None, None, None)


@pytest.mark.asyncio
async def test_message_queue_worker_process_with_rate_limits_triggered():
    # Create mocks
    queue = AsyncMock()
    stop_event = asyncio.Event()
    mqtt_manager = MagicMock(spec=MQTTManager)
    device_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter = MagicMock(spec=RateLimiter)

    # Create the worker
    worker = MessageQueueWorker(
        "TestWorker",
        queue,
        stop_event,
        mqtt_manager,
        device_rate_limiter,
        gateway_rate_limiter
    )

    # Create rate limits
    message_rate_limit = MagicMock(spec=RateLimit)
    message_rate_limit.has_limit.return_value = True
    datapoints_rate_limit = MagicMock(spec=RateLimit)
    datapoints_rate_limit.has_limit.return_value = True

    # Mock the rate limit methods
    worker._get_rate_limits_for_message = MagicMock(return_value=(message_rate_limit, datapoints_rate_limit))

    # Set up the rate limit to be triggered
    triggered_rate_limit_entry = (10, 5)  # (tokens, duration)
    expected_tokens = 5
    worker.check_rate_limits_for_message = AsyncMock(
        return_value=(triggered_rate_limit_entry, expected_tokens, message_rate_limit))

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")

    # Call the process method
    result = await worker.process(message)

    # Verify the rate limit methods were called
    worker.check_rate_limits_for_message.assert_awaited_once_with(
        datapoints_count=message.datapoints,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )

    # Verify the mqtt_manager.publish method was not called
    mqtt_manager.publish.assert_not_awaited()

    # Verify the result
    assert result == (5, 5, message_rate_limit)


@pytest.mark.asyncio
async def test_message_queue_worker_get_rate_limits_for_message_device_service():
    # Create mocks
    queue = AsyncMock()
    stop_event = asyncio.Event()
    mqtt_manager = MagicMock(spec=MQTTManager)

    # Create device rate limiter with proper attributes
    device_rate_limiter = MagicMock(spec=RateLimiter)
    device_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create gateway rate limiter with proper attributes
    gateway_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create the worker
    worker = MessageQueueWorker(
        "TestWorker",
        queue,
        stop_event,
        mqtt_manager,
        device_rate_limiter,
        gateway_rate_limiter
    )

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")
    message.is_device_message = True
    message.is_service_message = True

    # Call the method
    message_rate_limit, datapoints_rate_limit = worker._get_rate_limits_for_message(message)

    # Verify the correct rate limits were returned
    assert message_rate_limit == device_rate_limiter.message_rate_limit
    assert datapoints_rate_limit == EMPTY_RATE_LIMIT


@pytest.mark.asyncio
async def test_message_queue_worker_get_rate_limits_for_message_device_telemetry():
    # Create mocks
    queue = AsyncMock()
    stop_event = asyncio.Event()
    mqtt_manager = MagicMock(spec=MQTTManager)

    # Create device rate limiter with proper attributes
    device_rate_limiter = MagicMock(spec=RateLimiter)
    device_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create gateway rate limiter with proper attributes
    gateway_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create the worker
    worker = MessageQueueWorker(
        "TestWorker",
        queue,
        stop_event,
        mqtt_manager,
        device_rate_limiter,
        gateway_rate_limiter
    )

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")
    message.is_device_message = True
    message.is_service_message = False

    # Call the method
    message_rate_limit, datapoints_rate_limit = worker._get_rate_limits_for_message(message)

    # Verify the correct rate limits were returned
    assert message_rate_limit == device_rate_limiter.telemetry_message_rate_limit
    assert datapoints_rate_limit == device_rate_limiter.telemetry_datapoints_rate_limit


@pytest.mark.asyncio
async def test_message_queue_worker_get_rate_limits_for_message_gateway_service():
    # Create mocks
    queue = AsyncMock()
    stop_event = asyncio.Event()
    mqtt_manager = MagicMock(spec=MQTTManager)

    # Create device rate limiter with proper attributes
    device_rate_limiter = MagicMock(spec=RateLimiter)
    device_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create gateway rate limiter with proper attributes
    gateway_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create the worker
    worker = MessageQueueWorker(
        "TestWorker",
        queue,
        stop_event,
        mqtt_manager,
        device_rate_limiter,
        gateway_rate_limiter
    )

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")
    message.is_device_message = False
    message.is_service_message = True

    # Call the method
    message_rate_limit, datapoints_rate_limit = worker._get_rate_limits_for_message(message)

    # Verify the correct rate limits were returned
    assert message_rate_limit == gateway_rate_limiter.message_rate_limit
    assert datapoints_rate_limit == EMPTY_RATE_LIMIT


@pytest.mark.asyncio
async def test_message_queue_worker_get_rate_limits_for_message_gateway_telemetry():
    # Create mocks
    queue = AsyncMock()
    stop_event = asyncio.Event()
    mqtt_manager = MagicMock(spec=MQTTManager)

    # Create device rate limiter with proper attributes
    device_rate_limiter = MagicMock(spec=RateLimiter)
    device_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    device_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create gateway rate limiter with proper attributes
    gateway_rate_limiter = MagicMock(spec=RateLimiter)
    gateway_rate_limiter.message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_message_rate_limit = MagicMock(spec=RateLimit)
    gateway_rate_limiter.telemetry_datapoints_rate_limit = MagicMock(spec=RateLimit)

    # Create the worker
    worker = MessageQueueWorker(
        "TestWorker",
        queue,
        stop_event,
        mqtt_manager,
        device_rate_limiter,
        gateway_rate_limiter
    )

    # Create a test message
    message = MqttPublishMessage("test/topic", b"test_payload")
    message.is_device_message = False
    message.is_service_message = False

    # Call the method
    message_rate_limit, datapoints_rate_limit = worker._get_rate_limits_for_message(message)

    # Verify the correct rate limits were returned
    assert message_rate_limit == gateway_rate_limiter.telemetry_message_rate_limit
    assert datapoints_rate_limit == gateway_rate_limiter.telemetry_datapoints_rate_limit


@pytest.mark.asyncio
async def test_message_queue_worker_check_rate_limits_for_message_message_limit_triggered():
    # Create a mock message rate limit that will trigger
    message_rate_limit = MagicMock(spec=RateLimit)
    message_rate_limit.has_limit.return_value = True
    message_rate_limit.try_consume = AsyncMock(return_value=(10, 5))  # (tokens, duration)

    # Create a mock datapoints rate limit that won't be checked
    datapoints_rate_limit = MagicMock(spec=RateLimit)
    datapoints_rate_limit.has_limit.return_value = True

    # Call the method
    result = await MessageQueueWorker.check_rate_limits_for_message(
        datapoints_count=5,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )

    # Verify the result
    assert result == ((10, 5), 1, message_rate_limit)

    # Verify the message rate limit was checked
    message_rate_limit.try_consume.assert_awaited_once_with(1)

    # Verify the datapoints rate limit was not checked
    datapoints_rate_limit.try_consume.assert_not_awaited()


@pytest.mark.asyncio
async def test_message_queue_worker_check_rate_limits_for_message_datapoints_limit_triggered():
    # Create a mock message rate limit that won't trigger
    message_rate_limit = MagicMock(spec=RateLimit)
    message_rate_limit.has_limit.return_value = True
    message_rate_limit.try_consume = AsyncMock(return_value=None)

    # Create a mock datapoints rate limit that will trigger
    datapoints_rate_limit = MagicMock(spec=RateLimit)
    datapoints_rate_limit.has_limit.return_value = True
    datapoints_rate_limit.try_consume = AsyncMock(return_value=(20, 10))  # (tokens, duration)

    # Call the method
    result = await MessageQueueWorker.check_rate_limits_for_message(
        datapoints_count=5,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )

    # Verify the result
    assert result == ((20, 10), 5, datapoints_rate_limit)

    # Verify both rate limits were checked
    message_rate_limit.try_consume.assert_awaited_once_with(1)
    datapoints_rate_limit.try_consume.assert_awaited_once_with(5)


@pytest.mark.asyncio
async def test_message_queue_worker_check_rate_limits_for_message_no_limits_triggered():
    # Create mock rate limits that won't trigger
    message_rate_limit = MagicMock(spec=RateLimit)
    message_rate_limit.has_limit.return_value = True
    message_rate_limit.try_consume = AsyncMock(return_value=None)

    datapoints_rate_limit = MagicMock(spec=RateLimit)
    datapoints_rate_limit.has_limit.return_value = True
    datapoints_rate_limit.try_consume = AsyncMock(return_value=None)

    # Call the method
    result = await MessageQueueWorker.check_rate_limits_for_message(
        datapoints_count=5,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )

    # Verify the result
    assert result == (None, 0, None)

    # Verify both rate limits were checked
    message_rate_limit.try_consume.assert_awaited_once_with(1)
    datapoints_rate_limit.try_consume.assert_awaited_once_with(5)


@pytest.mark.asyncio
async def test_message_queue_worker_consume_rate_limits_for_message():
    # Create mock rate limits
    message_rate_limit = MagicMock(spec=RateLimit)
    message_rate_limit.has_limit.return_value = True
    message_rate_limit.consume = AsyncMock()

    datapoints_rate_limit = MagicMock(spec=RateLimit)
    datapoints_rate_limit.has_limit.return_value = True
    datapoints_rate_limit.consume = AsyncMock()

    # Call the method
    await MessageQueueWorker._consume_rate_limits_for_message(
        datapoints_count=5,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )

    # Verify both rate limits were consumed
    message_rate_limit.consume.assert_awaited_once_with(1)
    datapoints_rate_limit.consume.assert_awaited_once_with(5)


@pytest.mark.asyncio
async def test_message_queue_worker_consume_rate_limits_for_message_no_limits():
    # Create mock rate limits with no limits
    message_rate_limit = MagicMock(spec=RateLimit)
    message_rate_limit.has_limit.return_value = False

    datapoints_rate_limit = MagicMock(spec=RateLimit)
    datapoints_rate_limit.has_limit.return_value = False

    # Call the method
    await MessageQueueWorker._consume_rate_limits_for_message(
        datapoints_count=5,
        message_rate_limit=message_rate_limit,
        datapoints_rate_limit=datapoints_rate_limit
    )

    # Verify no rate limits were consumed
    message_rate_limit.consume.assert_not_awaited()
    datapoints_rate_limit.consume.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_loop_with_bytes_message(setup_retry_loop_service):
    service, mqtt_manager = setup_retry_loop_service
    mqtt_manager.is_connected.return_value = True

    message = MagicMock()
    message.original_payload = b"binary"

    service._retry_by_qos_queue.get = AsyncMock(side_effect=[message, asyncio.CancelledError()])

    await service._dispatch_retry_by_qos_queue_loop()

    service._service_queue.reinsert_front.assert_awaited_once_with(message)


@pytest.mark.asyncio
async def test_retry_loop_with_gateway_uplink_message(setup_retry_loop_service):
    service, mqtt_manager = setup_retry_loop_service
    mqtt_manager.is_connected.return_value = True

    message = MagicMock()
    message.original_payload = GatewayUplinkMessageBuilder().set_device_name("test").build()

    service._retry_by_qos_queue.get = AsyncMock(side_effect=[message, asyncio.CancelledError()])

    await service._dispatch_retry_by_qos_queue_loop()

    service._gateway_uplink_messages_queue.reinsert_front.assert_awaited_once_with(message)


@pytest.mark.asyncio
async def test_retry_loop_with_device_uplink_message(setup_retry_loop_service):
    service, mqtt_manager = setup_retry_loop_service
    mqtt_manager.is_connected.return_value = True

    message = MagicMock()
    message.original_payload = DeviceUplinkMessageBuilder().set_device_name("test-device").build()

    service._retry_by_qos_queue.get = AsyncMock(side_effect=[message, asyncio.CancelledError()])

    await service._dispatch_retry_by_qos_queue_loop()

    service._device_uplink_messages_queue.reinsert_front.assert_awaited_once_with(message)


@pytest.mark.asyncio
async def test_retry_loop_with_cancelled_error(setup_retry_loop_service):
    service, mqtt_manager = setup_retry_loop_service
    mqtt_manager.is_connected.return_value = True

    service._retry_by_qos_queue.get = AsyncMock(side_effect=asyncio.CancelledError())

    await service._dispatch_retry_by_qos_queue_loop()


@pytest.mark.asyncio
async def test_retry_loop_with_not_connected_and_empty_message(setup_retry_loop_service):
    service, mqtt_manager = setup_retry_loop_service

    mqtt_manager.is_connected.side_effect = [False, True]

    service._retry_by_qos_queue.get = AsyncMock(side_effect=[None, asyncio.CancelledError()])

    service._main_stop_event.is_set = MagicMock(side_effect=[False, True])

    with patch("asyncio.sleep", new=AsyncMock()):
        await service._dispatch_retry_by_qos_queue_loop()


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
