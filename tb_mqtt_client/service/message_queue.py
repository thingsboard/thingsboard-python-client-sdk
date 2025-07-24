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
from contextlib import suppress
from typing import List, Optional

from tb_mqtt_client.common.logging_utils import get_logger, TRACE_LEVEL
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessage
from tb_mqtt_client.service.device.message_adapter import MessageAdapter
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter
from tb_mqtt_client.service.mqtt_manager import MQTTManager

logger = get_logger(__name__)


class MessageQueue:
    _BATCH_TIMEOUT = 0.01  # seconds to wait for batching (optional flush time)

    def __init__(self,
                 mqtt_manager: MQTTManager,
                 main_stop_event: asyncio.Event,
                 message_rate_limit: Optional[RateLimit],
                 telemetry_rate_limit: Optional[RateLimit],
                 telemetry_dp_rate_limit: Optional[RateLimit],
                 message_adapter: MessageAdapter,
                 max_queue_size: int = 1000000,
                 batch_collect_max_time_ms: int = 100,
                 batch_collect_max_count: int = 500,
                 gateway_message_adapter: Optional[GatewayMessageAdapter] = None):
        self._main_stop_event = main_stop_event
        self._batch_max_time = batch_collect_max_time_ms / 1000
        self._batch_max_count = batch_collect_max_count
        self._mqtt_manager = mqtt_manager
        self._message_rate_limit = message_rate_limit
        self._telemetry_rate_limit = telemetry_rate_limit
        self._telemetry_dp_rate_limit = telemetry_dp_rate_limit
        self._backpressure = self._mqtt_manager.backpressure
        # Queue expects tuples of (mqtt_message, delivery_futures)
        self._queue: asyncio.Queue[MqttPublishMessage] = asyncio.Queue(maxsize=max_queue_size)
        self._pending_queue_tasks: set[asyncio.Task] = set()
        self._active = asyncio.Event()
        self._wakeup_event = asyncio.Event()
        self._retry_tasks: set[asyncio.Task] = set()
        self._active.set()
        self._adapter = message_adapter
        self._gateway_adapter = gateway_message_adapter
        self._loop_task = asyncio.create_task(self._dequeue_loop())
        self._rate_limit_refill_task = asyncio.create_task(self._rate_limit_refill_loop())
        self.__print_queue_statistics_task = asyncio.create_task(self.print_queue_statistics())
        logger.debug("MessageQueue initialized: max_queue_size=%s, batch_time=%.3f, batch_count=%d",
                     max_queue_size, self._batch_max_time, batch_collect_max_count)

    async def publish(self, message: MqttPublishMessage) -> Optional[List[asyncio.Future[PublishResult]]]:
        try:
            if logger.isEnabledFor(TRACE_LEVEL):
                logger.trace(f"Pushing message to queue with delivery futures: {[f.uuid for f in message.delivery_futures]}")
            self._queue.put_nowait(message)
        except asyncio.QueueFull:
            logger.error("Message queue full. Dropping message for topic %s", message.topic)
            for future in message.delivery_futures:
                if future:
                    future.set_result(PublishResult(message.topic, message.qos, -1, len(message.payload), -1))

    async def _dequeue_loop(self):
        logger.debug("MessageQueue dequeue loop started.")
        while self._active.is_set() and not self._main_stop_event.is_set():
            try:
                try:
                    message = await self._wait_for_message()
                    if logger.isEnabledFor(TRACE_LEVEL):
                        logger.trace(f"Dequed message with delivery futures: {[f.uuid for f in message.delivery_futures]}")
                    await asyncio.sleep(0)  # cooperative yield
                except asyncio.TimeoutError:
                    logger.trace("Dequeue wait timed out. Yielding...")
                    await asyncio.sleep(0.001)
                    continue
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warning("Unexpected error in dequeue loop: %s", e)
                    continue

                if isinstance(message, MqttPublishMessage) and isinstance(message.payload, bytes):
                    logger.trace("Dequeued immediate publish: topic=%s (raw bytes)", message.topic)
                    await self._try_publish(message)
                    continue

                logger.trace("Dequeued message for batching: topic=%s, device=%s",
                             message.topic, getattr(message.payload, 'device_name', 'N/A'))

                batch: List[MqttPublishMessage] = [message]
                start = asyncio.get_event_loop().time()
                batch_size = message.payload.size
                batch_type = type(message.payload).__name__

                while not self._queue.empty():
                    elapsed = asyncio.get_event_loop().time() - start
                    if elapsed >= self._batch_max_time:
                        logger.trace("Batch time threshold reached: %.3fs", elapsed)
                        break
                    if len(batch) >= self._batch_max_count:
                        logger.trace("Batch count threshold reached: %d messages", len(batch))
                        break

                    try:
                        next_message = self._queue.get_nowait()
                        if isinstance(next_message.payload, DeviceUplinkMessage) or isinstance(next_message.payload, GatewayUplinkMessage):
                            if batch_type is not None and batch_type != type(next_message.payload).__name__:
                                logger.trace("Batch type mismatch: current=%s, next=%s, finalizing current",
                                             batch_type, type(next_message.payload).__class__.__name__)
                                self._queue.put_nowait(next_message)
                                break
                            batch_type = type(next_message.payload).__name__
                            msg_size = next_message.payload.size
                            if batch_size + msg_size > self._adapter.splitter.max_payload_size:  # noqa
                                logger.trace("Batch size threshold exceeded: current=%d, next=%d", batch_size, msg_size)
                                self._queue.put_nowait(next_message)
                                break
                            batch.append(next_message)
                            batch_size += msg_size
                        else:
                            logger.trace("Immediate publish encountered in queue while batching: topic=%s", next_message.topic)
                            await self._try_publish(next_message)
                    except asyncio.QueueEmpty:
                        break

                if batch_type is None:
                    batch_type = type(message.payload).__name__

                if batch:
                    logger.trace("Batching completed: %d messages, total size=%d", len(batch), batch_size)

                    if batch_type == 'GatewayUplinkMessage' and self._gateway_adapter:
                        logger.trace("Building gateway uplink payloads for %d messages", len(batch))
                        topic_payloads = self._gateway_adapter.build_uplink_messages(batch)
                    else:
                        topic_payloads = self._adapter.build_uplink_messages(batch)

                    for built_message in topic_payloads:
                        logger.trace("Dispatching batched message: topic=%s, size=%d, datapoints=%d, delivery_futures=%r",
                                     built_message.topic,
                                     len(built_message.payload),
                                     built_message.datapoints,
                                     [f.uuid for f in built_message.delivery_futures])
                        await self._try_publish(built_message)

            except Exception as e:
                logger.error("Error in dequeue loop:", exc_info=e)
                logger.debug("Dequeue loop error details:", exc_info=e)
                if isinstance(e, asyncio.CancelledError):
                    break
                continue

    async def _try_publish(self, message: MqttPublishMessage):
        is_message_with_telemetry_or_attributes = message.topic in (mqtt_topics.DEVICE_TELEMETRY_TOPIC,
                                                                    mqtt_topics.DEVICE_ATTRIBUTES_TOPIC)
        # TODO: Add topics check for gateways

        logger.trace("Attempting publish: topic=%s, datapoints=%d", message.topic, message.datapoints)

        # Check backpressure first - if active, don't even try to check rate limits
        if self._backpressure.should_pause():
            logger.debug("Backpressure active, delaying publish of topic=%s for %.1f seconds", message.topic, 1.0)
            self._schedule_delayed_retry(message)
            return

        # Check and consume rate limits atomically before publishing
        if is_message_with_telemetry_or_attributes:
            # For telemetry messages, we need to check both message and datapoint rate limits
            telemetry_msg_success = True
            telemetry_dp_success = True

            if self._telemetry_rate_limit:
                triggered_rate_limit = await self._telemetry_rate_limit.try_consume(1)
                if triggered_rate_limit:
                    logger.debug("Telemetry message rate limit hit for topic %s: %r per %r seconds",
                                 message.topic, triggered_rate_limit[0], triggered_rate_limit[1])
                    retry_delay = self._telemetry_rate_limit.minimal_timeout
                    self._schedule_delayed_retry(message, delay=retry_delay)
                    return

            if self._telemetry_dp_rate_limit:
                triggered_rate_limit = await self._telemetry_dp_rate_limit.try_consume(message.datapoints)
                if triggered_rate_limit:
                    logger.debug("Telemetry datapoint rate limit hit for topic %s: %r per %r seconds",
                                 message.topic, triggered_rate_limit[0], triggered_rate_limit[1])
                    retry_delay = self._telemetry_dp_rate_limit.minimal_timeout
                    self._schedule_delayed_retry(message, delay=retry_delay)
                    return
        else:
            # For non-telemetry messages, we only need to check the message rate limit
            if self._message_rate_limit:
                triggered_rate_limit = await self._message_rate_limit.try_consume(1)
                if triggered_rate_limit:
                    logger.debug("Generic message rate limit hit for topic %s: %r per %r seconds",
                                 message.topic, triggered_rate_limit[0], triggered_rate_limit[1])
                    retry_delay = self._message_rate_limit.minimal_timeout
                    self._schedule_delayed_retry(message, delay=retry_delay)
                    return
        try:
            if logger.isEnabledFor(TRACE_LEVEL):
                logger.trace("Trying to publish topic=%s, payload size=%d, attached future id=%r",
                                 message.topic, len(message.payload), [f.uuid for f in message.delivery_futures])

            await self._mqtt_manager.publish(message)

        except Exception as e:
            logger.warning("Failed to publish to topic %s: %s. Scheduling retry.", message.topic, e)
            logger.debug("Scheduling retry for topic=%s, payload size=%d, qos=%d",
                         message.topic, len(message.payload), message.qos)
            logger.debug("error details: %s", e, exc_info=True)
            self._schedule_delayed_retry(message, delay=.1)

    def _schedule_delayed_retry(self, message: MqttPublishMessage, delay: float = 0.1):
        if not self._active.is_set() or self._main_stop_event.is_set():
            logger.debug("MessageQueue is not active or main stop event is set. Not scheduling retry for topic=%s", message.topic)
            return
        logger.trace("Scheduling retry: topic=%s, delay=%.2f", message.topic, delay)

        task = asyncio.create_task(self.__retry_task(message, delay))
        self._retry_tasks.add(task)
        task.add_done_callback(self._retry_tasks.discard)

    async def __retry_task(self, message: MqttPublishMessage, delay: float):
        try:
            logger.debug("Retrying publish: topic=%s", message.topic)
            await asyncio.sleep(delay)
            if not self._active.is_set() or self._main_stop_event.is_set():
                logger.debug(
                    "MessageQueue is not active or main stop event is set. Not re-enqueuing message for topic=%s",
                    message.topic)
                return
            self._queue.put_nowait(message)
            self._wakeup_event.set()
            logger.debug("Re-enqueued message after delay: topic=%s", message.topic)
        except asyncio.QueueFull:
            logger.warning("Retry queue full. Dropping retried message: topic=%s", message.topic)
        except Exception as e:
            logger.debug("Unexpected error during delayed retry: %s", e)

    async def _wait_for_message(self) -> MqttPublishMessage:
        while self._active.is_set():
            try:
                if not self._queue.empty():
                    try:
                        return await self._queue.get()
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(.01)

                self._wakeup_event.clear()
                queue_task = asyncio.create_task(self._queue.get())
                self._pending_queue_tasks.add(queue_task)

                wake_task = asyncio.create_task(self._wakeup_event.wait())

                done, pending = await asyncio.wait([queue_task, wake_task], return_when=asyncio.FIRST_COMPLETED)

                for task in pending:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task

                self._pending_queue_tasks.discard(queue_task)

                if queue_task in done:
                    logger.trace("Retrieved message from queue: %r", queue_task.result())
                    return queue_task.result()

                await asyncio.sleep(0)  # Yield control to the event loop

            except asyncio.CancelledError:
                break

        raise asyncio.CancelledError("MessageQueue is shutting down or stopped.")

    async def shutdown(self):
        logger.debug("Shutting down MessageQueue...")
        self._active.clear()
        self._wakeup_event.set()  # Wake up the _wait_for_message if it's blocked

        await self._cancel_tasks(self._retry_tasks)
        await self._cancel_tasks(self._pending_queue_tasks)

        self._loop_task.cancel()
        if self._rate_limit_refill_task:
            self._rate_limit_refill_task.cancel()
        self.__print_queue_statistics_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._loop_task
            await self._rate_limit_refill_task
            await self.__print_queue_statistics_task

        self.clear()

        logger.debug("MessageQueue shutdown complete, message queue size: %d",
                        self._queue.qsize())

    @staticmethod
    async def _cancel_tasks(tasks: set[asyncio.Task]):
        for task in list(tasks):
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks, return_exceptions=True)
        tasks.clear()

    def is_empty(self):
        return self._queue.empty()

    def clear(self):
        logger.debug("Clearing message queue...")
        while not self._queue.empty():
            message = self._queue.get_nowait()
            for future in message.delivery_futures:
                future.set_result(PublishResult(
                    topic=message.topic,
                    qos=message.qos,
                    message_id=-1,
                    payload_size=message.payload_size,
                    reason_code=-1
                ))
            self._queue.task_done()
        logger.debug("Message queue cleared.")

    async def _rate_limit_refill_loop(self):
        try:
            while self._active.is_set():
                await asyncio.sleep(1.0)
                await self._refill_rate_limits()
                logger.trace("Rate limits refilled, state: %s",
                             {
                                 "message_rate_limit": self._message_rate_limit.to_dict() if self._message_rate_limit else None,
                                 "telemetry_rate_limit": self._telemetry_rate_limit.to_dict() if self._telemetry_rate_limit else None,
                                 "telemetry_dp_rate_limit": self._telemetry_dp_rate_limit.to_dict() if self._telemetry_dp_rate_limit else None
                             })
        except asyncio.CancelledError:
            pass

    async def _refill_rate_limits(self):
        for rl in (self._message_rate_limit, self._telemetry_rate_limit, self._telemetry_dp_rate_limit):
            if rl:
                await rl.refill()

    def set_gateway_message_adapter(self, message_adapter: GatewayMessageAdapter):
        self._gateway_adapter = message_adapter


    async def print_queue_statistics(self):
        """
        Prints the current statistics of the message queue.
        """
        while self._active.is_set() and not self._main_stop_event.is_set():
            queue_size = self._queue.qsize()
            pending_tasks = len(self._pending_queue_tasks)
            retry_tasks = len(self._retry_tasks)
            active = self._active.is_set()
            logger.info("MessageQueue Statistics: "
                        "Queue Size: %d, Pending Tasks: %d, Retry Tasks: %d, Active: %s",
                        queue_size, pending_tasks, retry_tasks, active)
            await asyncio.sleep(60)
