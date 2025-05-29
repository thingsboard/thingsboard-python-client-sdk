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
from typing import List, Optional, Union, Tuple, Dict, Callable

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.entities.publish_result import PublishResult
from tb_mqtt_client.service.message_dispatcher import MessageDispatcher
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
                 message_dispatcher: MessageDispatcher,
                 max_queue_size: int = 1000000,
                 batch_collect_max_time_ms: int = 100,
                 batch_collect_max_count: int = 500):
        self.__qos = 1
        self._main_stop_event = main_stop_event
        self._batch_max_time = batch_collect_max_time_ms / 1000  # convert to seconds
        self._batch_max_count = batch_collect_max_count
        self._mqtt_manager = mqtt_manager
        self._message_rate_limit = message_rate_limit
        self._telemetry_rate_limit = telemetry_rate_limit
        self._telemetry_dp_rate_limit = telemetry_dp_rate_limit
        self._backpressure = self._mqtt_manager.backpressure
        self._pending_ack_futures: Dict[int, asyncio.Future[PublishResult]] = {}
        self._pending_ack_callbacks: Dict[int, Callable[[bool], None]] = {}
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._active = asyncio.Event()
        self._wakeup_event = asyncio.Event()
        self._retry_tasks: set[asyncio.Task] = set()
        self._active.set()
        self._dispatcher = message_dispatcher
        self._loop_task = asyncio.create_task(self._dequeue_loop())
        self._rate_limit_refill_task = asyncio.create_task(self._rate_limit_refill_loop())
        logger.debug("MessageQueue initialized: max_queue_size=%s, batch_time=%.3f, batch_count=%d",
                     max_queue_size, self._batch_max_time, batch_collect_max_count)

    async def publish(self, topic: str, payload: Union[bytes, DeviceUplinkMessage], datapoints_count: int):
        delivery_futures = payload.get_delivery_futures() if isinstance(payload, DeviceUplinkMessage) else []
        try:
            logger.trace("publish() received delivery future id: %r for topic=%s",
                         id(delivery_futures[0]) if delivery_futures else -1, topic)
            self._queue.put_nowait((topic, payload, delivery_futures, datapoints_count))
            logger.trace("Enqueued message: topic=%s, datapoints=%d, type=%s",
                         topic, datapoints_count, type(payload).__name__)
        except asyncio.QueueFull:
            logger.error("Message queue full. Dropping message for topic %s", topic)
            for future in payload.get_delivery_futures():
                if future:
                    future.set_result(False)
        return delivery_futures or None

    async def _dequeue_loop(self):
        logger.debug("MessageQueue dequeue loop started.")
        while self._active.is_set():
            try:
                topic, payload, delivery_futures_or_none, count = await asyncio.wait_for(asyncio.get_event_loop().create_task(self._queue.get()), timeout=self._BATCH_TIMEOUT)
                logger.trace("MessageQueue dequeue: topic=%s, payload=%r, count=%d",
                             topic, payload, count)
                if isinstance(payload, bytes):
                    await self._try_publish(topic, payload, count, delivery_futures_or_none)
                    continue
                logger.trace("Dequeued message: delivery_future id: %r topic=%s, type=%s, datapoints=%d",
                                 id(delivery_futures_or_none[0]) if delivery_futures_or_none else -1,
                             topic, type(payload).__name__, count)
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

            if isinstance(payload, bytes):
                logger.trace("Dequeued immediate publish: topic=%s (raw bytes)", topic)
                await self._try_publish(topic, payload, count, delivery_futures_or_none)
                continue

            logger.trace("Dequeued message for batching: topic=%s, device=%s",
                         topic, getattr(payload, 'device_name', 'N/A'))

            batch: List[Tuple[str, Union[bytes, DeviceUplinkMessage], asyncio.Future[PublishResult], int]] = [(topic, payload, delivery_futures_or_none, count)]
            start = asyncio.get_event_loop().time()
            batch_size = payload.size

            while not self._queue.empty():
                elapsed = asyncio.get_event_loop().time() - start
                if elapsed >= self._batch_max_time:
                    logger.trace("Batch time threshold reached: %.3fs", elapsed)
                    break
                if len(batch) >= self._batch_max_count:
                    logger.trace("Batch count threshold reached: %d messages", len(batch))
                    break

                try:
                    next_topic, next_payload, delivery_futures_or_none, next_count = self._queue.get_nowait()
                    if isinstance(next_payload, DeviceUplinkMessage):
                        msg_size = next_payload.size
                        if batch_size + msg_size > self._dispatcher.splitter.max_payload_size:  # noqa
                            logger.trace("Batch size threshold exceeded: current=%d, next=%d", batch_size, msg_size)
                            self._queue.put_nowait((next_topic, next_payload, delivery_futures_or_none, next_count))
                            break
                        batch.append((next_topic, next_payload, delivery_futures_or_none, next_count))
                        batch_size += msg_size
                    else:
                        logger.trace("Immediate publish encountered in queue while batching: topic=%s", next_topic)
                        await self._try_publish(next_topic, next_payload, next_count)
                except asyncio.QueueEmpty:
                    break

            if batch:
                logger.trace("Batching completed: %d messages, total size=%d", len(batch), batch_size)
                messages = [device_uplink_message for _, device_uplink_message, _, _ in batch]

                topic_payloads = self._dispatcher.build_uplink_payloads(messages)

                for topic, payload, datapoints, delivery_futures in topic_payloads:
                    logger.trace("Dispatching batched message: topic=%s, size=%d, datapoints=%d, delivery_futures=%r",
                                 topic, len(payload), datapoints, [id(f) for f in delivery_futures])
                    await self._try_publish(topic, payload, datapoints, delivery_futures)

    async def _try_publish(self,
                           topic: str,
                           payload: bytes,
                           datapoints: int,
                           delivery_futures_or_none: List[Optional[asyncio.Future[PublishResult]]] = None):
        if delivery_futures_or_none is None:
            logger.trace("No delivery futures associated! This publish result will not be tracked.")
            delivery_futures_or_none = []
        is_message_with_telemetry_or_attributes = topic in (mqtt_topics.DEVICE_TELEMETRY_TOPIC,
                                                            mqtt_topics.DEVICE_ATTRIBUTES_TOPIC)

        logger.trace("Attempting publish: topic=%s, datapoints=%d", topic, datapoints)

        # Check backpressure first - if active, don't even try to check rate limits
        if self._backpressure.should_pause():
            logger.debug("Backpressure active, delaying publish of topic=%s for %.1f seconds", topic, 1.0)
            self._schedule_delayed_retry(topic, payload, datapoints, delay=1.0, delivery_futures=delivery_futures_or_none)
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
                                 topic, triggered_rate_limit[0], triggered_rate_limit[1])
                    retry_delay = self._telemetry_rate_limit.minimal_timeout
                    self._schedule_delayed_retry(topic, payload, datapoints, delay=retry_delay, delivery_futures=delivery_futures_or_none)
                    return

            if self._telemetry_dp_rate_limit:
                triggered_rate_limit = await self._telemetry_dp_rate_limit.try_consume(datapoints)
                if triggered_rate_limit:
                    logger.debug("Telemetry datapoint rate limit hit for topic %s: %r per %r seconds",
                                 topic, triggered_rate_limit[0], triggered_rate_limit[1])
                    retry_delay = self._telemetry_dp_rate_limit.minimal_timeout
                    self._schedule_delayed_retry(topic, payload, datapoints, delay=retry_delay, delivery_futures=delivery_futures_or_none)
                    return
        else:
            # For non-telemetry messages, we only need to check the message rate limit
            if self._message_rate_limit:
                triggered_rate_limit = await self._message_rate_limit.try_consume(1)
                if triggered_rate_limit:
                    logger.debug("Generic message rate limit hit for topic %s: %r per %r seconds",
                                 topic, triggered_rate_limit[0], triggered_rate_limit[1])
                    retry_delay = self._message_rate_limit.minimal_timeout
                    self._schedule_delayed_retry(topic,
                                                 payload,
                                                 datapoints,
                                                 delay=retry_delay,
                                                 delivery_futures=delivery_futures_or_none)
                    return
        try:
            logger.trace("Trying to publish topic=%s, payload size=%d, attached future id=%r",
                             topic, len(payload), id(delivery_futures_or_none[0]) if delivery_futures_or_none else -1)

            mqtt_future = await self._mqtt_manager.publish(topic, payload, qos=self.__qos)

            if delivery_futures_or_none is not None:
                def resolve_attached(publish_future: asyncio.Future):
                    try:
                        publish_result = publish_future.result()
                    except Exception as exc:
                        logger.warning("Publish failed with exception: %s", exc)
                        logger.debug("Resolving delivery futures with failure:", exc_info=exc)
                        publish_result = PublishResult(topic, self.__qos, -1, len(payload), -1)

                    for i, f in enumerate(delivery_futures_or_none):
                        if f is not None and not f.done():
                            f.set_result(publish_result)
                            logger.trace("Resolved delivery future #%d id=%r with %s, main publish future id: %r, %r",
                                         i, id(f), publish_result, id(publish_future), publish_future)

                logger.trace("Adding done callback to main publish future: %r, main publish future done state: %r", id(mqtt_future), mqtt_future.done())
                mqtt_future.add_done_callback(resolve_attached)
        except Exception as e:
            logger.warning("Failed to publish to topic %s: %s. Scheduling retry.", topic, e)
            self._schedule_delayed_retry(topic, payload, datapoints, delay=.1)

    def _schedule_delayed_retry(self, topic: str, payload: bytes, points: int, delay: float,
                                delivery_futures: Optional[List[Optional[asyncio.Future[PublishResult]]]] = None):
        if not self._active.is_set() or self._main_stop_event.is_set():
            logger.debug("MessageQueue is not active or main stop event is set. Not scheduling retry for topic=%s", topic)
            return
        logger.trace("Scheduling retry: topic=%s, delay=%.2f", topic, delay)

        async def retry():
            try:
                logger.debug("Retrying publish: topic=%s", topic)
                await asyncio.sleep(delay)
                if not self._active.is_set() or self._main_stop_event.is_set():
                    logger.debug("MessageQueue is not active or main stop event is set. Not re-enqueuing message for topic=%s", topic)
                    return
                self._queue.put_nowait((topic, payload, delivery_futures, points))
                self._wakeup_event.set()
                logger.debug("Re-enqueued message after delay: topic=%s", topic)
            except asyncio.QueueFull:
                logger.warning("Retry queue full. Dropping retried message: topic=%s", topic)
            except Exception as e:
                logger.debug("Unexpected error during delayed retry: %s", e)

        task = asyncio.create_task(retry())
        self._retry_tasks.add(task)
        task.add_done_callback(self._retry_tasks.discard)

    async def _wait_for_message(self) -> Tuple[str, Union[bytes, DeviceUplinkMessage], int]:
        while self._active.is_set():
            try:
                if not self._queue.empty():
                    try:
                        return await self._queue.get()
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(.01)

                self._wakeup_event.clear()
                queue_task = asyncio.create_task(self._queue.get())
                wake_task = asyncio.create_task(self._wakeup_event.wait())

                done, pending = await asyncio.wait(
                    [queue_task, wake_task], return_when=asyncio.FIRST_COMPLETED
                )

                for task in pending:
                    logger.debug("Cancelling pending task: %r, it is queue_task = %r", task, queue_task==task)
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task

                if queue_task in done:
                    logger.debug("Retrieved message from queue: %r", queue_task.result())
                    return queue_task.result()

                await asyncio.sleep(0)

            except asyncio.CancelledError:
                break

        raise asyncio.CancelledError("MessageQueue is shutting down or stopped.")

    async def shutdown(self):
        logger.debug("Shutting down MessageQueue...")
        self._active.clear()
        self._wakeup_event.set()  # Wake up the _wait_for_message if it's blocked

        for task in list(self._retry_tasks):
            try:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(*self._retry_tasks, return_exceptions=True)
            except Exception as e:
                logger.warning("Error while cancelling retry task: %s", e)

        self._loop_task.cancel()
        self._rate_limit_refill_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._loop_task
            await self._rate_limit_refill_task

        logger.debug("MessageQueue shutdown complete.")

    def is_empty(self):
        return self._queue.empty()

    def size(self):
        return self._queue.qsize()

    def clear(self):
        logger.debug("Clearing message queue...")
        while not self._queue.empty():
            _, message, _ = self._queue.get_nowait()
            if isinstance(message, DeviceUplinkMessage) and message.get_delivery_futures():
                for future in message.get_delivery_futures():
                    future.set_result(False)
            self._queue.task_done()
        logger.debug("Message queue cleared.")

    @property
    def qos(self) -> int:
        return self.__qos

    @qos.setter
    def qos(self, qos: int):
        self.__qos = qos

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
