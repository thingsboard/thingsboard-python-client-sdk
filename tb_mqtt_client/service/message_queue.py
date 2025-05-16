#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#


import asyncio
from typing import List, Optional, Union, Tuple
from contextlib import suppress

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.service.mqtt_manager import MQTTManager
from tb_mqtt_client.service.message_dispatcher import MessageDispatcher

logger = get_logger(__name__)


class MessageQueue:
    _BATCH_TIMEOUT = 0.01  # seconds to wait for batching (optional flush time)

    def __init__(self,
                 mqtt_manager: MQTTManager,
                 message_rate_limit: Optional[RateLimit],
                 telemetry_rate_limit: Optional[RateLimit],
                 telemetry_dp_rate_limit: Optional[RateLimit],
                 message_dispatcher: MessageDispatcher,
                 max_queue_size: int = 10000,
                 batch_collect_max_time_ms: int = 100,
                 batch_collect_max_count: int = 500):
        self._batch_max_time = batch_collect_max_time_ms / 1000  # convert to seconds
        self._batch_max_count = batch_collect_max_count
        self._mqtt_manager = mqtt_manager
        self._message_rate_limit = message_rate_limit
        self._telemetry_rate_limit = telemetry_rate_limit
        self._telemetry_dp_rate_limit = telemetry_dp_rate_limit
        self._backpressure = self._mqtt_manager.backpressure
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._active = asyncio.Event()
        self._wakeup_event = asyncio.Event()
        self._active.set()
        self._dispatcher = message_dispatcher
        self._loop_task = asyncio.create_task(self._dequeue_loop())
        logger.debug("MessageQueue initialized: max_queue_size=%s, batch_time=%.3f, batch_count=%d",
                     max_queue_size, self._batch_max_time, batch_collect_max_count)

    async def publish(self, topic: str, payload: Union[bytes, DeviceUplinkMessage], datapoints_count: int):
        try:
            self._queue.put_nowait((topic, payload, datapoints_count))
            logger.trace("Enqueued message: topic=%s, datapoints=%d, type=%s",
                         topic, datapoints_count, type(payload).__name__)
        except asyncio.QueueFull:
            logger.warning("Message queue full. Dropping message for topic %s", topic)

    async def _dequeue_loop(self):
        while self._active.is_set():
            try:
                topic, payload, count = await self._wait_for_message()
            except asyncio.TimeoutError:
                continue

            if isinstance(payload, bytes):
                logger.trace("Dequeued immediate publish: topic=%s (raw bytes)", topic)
                await self._try_publish(topic, payload, count)
                continue

            logger.trace("Dequeued message for batching: topic=%s, device=%s",
                         topic, getattr(payload, 'device_name', 'N/A'))

            batch: List[Tuple[str, Union[bytes, DeviceUplinkMessage], int]] = [(topic, payload, count)]
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

                next_topic, next_payload, next_count = self._queue.get_nowait()
                if isinstance(next_payload, DeviceUplinkMessage):
                    msg_size = next_payload.size
                    if batch_size + msg_size > self._dispatcher.splitter.max_payload_size:
                        logger.trace("Batch size threshold exceeded: current=%d, next=%d", batch_size, msg_size)
                        await self._queue.put((next_topic, next_payload, next_count))
                        break
                    batch.append((next_topic, next_payload, next_count))
                    batch_size += msg_size
                else:
                    logger.trace("Immediate publish encountered in queue while batching: topic=%s", next_topic)
                    await self._try_publish(next_topic, next_payload, next_count)

            if batch:
                messages = [p for _, p, _ in batch if isinstance(p, DeviceUplinkMessage)]
                logger.trace("Formed batch with %d DeviceUplinkMessages", len(messages))
                topic_payloads = self._dispatcher.build_topic_payloads(messages)
                for topic, payload, datapoints in topic_payloads:
                    logger.trace("Dispatching batched message: topic=%s, size=%d, datapoints=%d",
                                 topic, len(payload), datapoints)
                    await self._try_publish(topic, payload, datapoints)

    async def _try_publish(self, topic: str, payload: bytes, points: int):
        telemetry = topic == mqtt_topics.DEVICE_TELEMETRY_TOPIC
        logger.trace("Attempting publish: topic=%s, datapoints=%d", topic, points)

        if self._backpressure.should_pause():
            self._schedule_delayed_retry(topic, payload, points, delay=1.0)
            return

        if telemetry:
            if self._telemetry_rate_limit and self._telemetry_rate_limit.check_limit_reached(1):
                logger.debug("Telemetry message rate limit hit: topic=%s", topic)
                retry_delay = self._telemetry_rate_limit.minimal_timeout
                self._schedule_delayed_retry(topic, payload, points, delay=retry_delay)
                return
            if self._telemetry_dp_rate_limit and self._telemetry_dp_rate_limit.check_limit_reached(points):
                logger.debug("Telemetry datapoint rate limit hit: topic=%s", topic)
                retry_delay = self._telemetry_dp_rate_limit.minimal_timeout
                self._schedule_delayed_retry(topic, payload, points, delay=retry_delay)
                return
        else:
            if self._message_rate_limit and self._message_rate_limit.check_limit_reached(1):
                logger.debug("Generic message rate limit hit: topic=%s", topic)
                logger.debug("Rate limit state: %s", self._message_rate_limit.to_dict())
                retry_delay = self._message_rate_limit.minimal_timeout
                self._schedule_delayed_retry(topic, payload, points, delay=retry_delay)
                return

        try:
            logger.debug("Rate limit state before publish: %s", self._message_rate_limit.to_dict())
            await self._mqtt_manager.publish(topic, payload, qos=1)
            logger.trace("Publish successful: topic=%s", topic)
            if telemetry:
                if self._telemetry_rate_limit:
                    self._telemetry_rate_limit.consume(1)
                if self._telemetry_dp_rate_limit:
                    self._telemetry_dp_rate_limit.consume(points)
            else:
                if self._message_rate_limit:
                    self._message_rate_limit.consume(1)
        except Exception as e:
            logger.warning("Failed to publish to topic %s: %s. Scheduling retry.", topic, e)
            self._schedule_delayed_retry(topic, payload, points, delay=1.0)

    def _schedule_delayed_retry(self, topic: str, payload: bytes, points: int, delay: float):
        logger.trace("Scheduling retry: topic=%s, delay=%.2f", topic, delay)

        async def retry():
            await asyncio.sleep(delay)
            try:
                self._queue.put_nowait((topic, payload, points))
                self._wakeup_event.set()
                logger.trace("Re-enqueued message after delay: topic=%s", topic)
            except asyncio.QueueFull:
                logger.warning("Retry queue full. Dropping retried message: topic=%s", topic)

        asyncio.create_task(retry())

    async def _wait_for_message(self):
        if not self._queue.empty():
            return await self._queue.get()

        self._wakeup_event.clear()
        queue_task = asyncio.create_task(self._queue.get())
        wake_task = asyncio.create_task(self._wakeup_event.wait())
        done, _ = await asyncio.wait([queue_task, wake_task], return_when=asyncio.FIRST_COMPLETED)

        if queue_task in done:
            wake_task.cancel()
            return queue_task.result()

        # Wake event triggered — retry get
        queue_task.cancel()
        await asyncio.sleep(0.001)  # Yield control
        return await self._wait_for_message()

    async def shutdown(self):
        logger.debug("Shutting down MessageQueue...")
        self._active.clear()
        self._loop_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._loop_task
        logger.debug("MessageQueue shutdown complete.")

    def is_empty(self):
        return self._queue.empty()

    def size(self):
        return self._queue.qsize()

    def clear(self):
        logger.debug("Clearing message queue...")
        while not self._queue.empty():
            self._queue.get_nowait()
            self._queue.task_done()
        logger.debug("Message queue cleared.")
