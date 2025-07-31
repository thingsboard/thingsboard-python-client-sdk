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
from typing import List, Optional, Tuple, Union

from tb_mqtt_client.common.logging_utils import get_logger, TRACE_LEVEL
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.common.queue import AsyncDeque
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit, EMPTY_RATE_LIMIT
from tb_mqtt_client.common.rate_limit.rate_limiter import RateLimiter
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessage
from tb_mqtt_client.service.device.message_adapter import MessageAdapter
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter
from tb_mqtt_client.service.mqtt_manager import MQTTManager

logger = get_logger(__name__)


class MessageService:
    _QUEUE_COOLDOWN = 0.01  # seconds to sleep before the next iteration in case of empty queues

    def __init__(self,
                 mqtt_manager: MQTTManager,
                 main_stop_event: asyncio.Event,
                 device_rate_limiter: RateLimiter,
                 message_adapter: MessageAdapter,
                 max_queue_size: int = 1000000,
                 batch_collect_max_time_ms: int = 10,
                 batch_collect_max_count: int = 500,
                 gateway_message_adapter: Optional[GatewayMessageAdapter] = None,
                 gateway_rate_limiter: Optional[RateLimiter] = None):
        self._main_stop_event = main_stop_event
        self._batch_max_time = batch_collect_max_time_ms / 1000
        self._batch_max_count = batch_collect_max_count
        self._mqtt_manager = mqtt_manager
        # Patching MQTT client to add an ability to use retry logic
        self._mqtt_manager.patch_client_for_retry_logic(self.put_retry_message)
        self._device_rate_limiter = device_rate_limiter
        self._gateway_rate_limiter = gateway_rate_limiter
        self._rate_limit_ready = asyncio.Event()
        self._backpressure = self._mqtt_manager.backpressure
        self._retry_by_qos_queue: AsyncDeque = AsyncDeque(maxlen=max_queue_size)
        self._initial_queue: AsyncDeque = AsyncDeque(maxlen=max_queue_size)
        self._service_queue: AsyncDeque = AsyncDeque(maxlen=max_queue_size)
        self._service_message_worker = MessageQueueWorker("ServiceMessageWorker",
                                                          self._service_queue,
                                                          self._main_stop_event,
                                                          self._mqtt_manager,
                                                          self._device_rate_limiter,
                                                          self._gateway_rate_limiter,)
        self._device_uplink_messages_queue: AsyncDeque = AsyncDeque(maxlen=max_queue_size)
        self._device_uplink_message_worker = MessageQueueWorker("DeviceUplinkMessageWorker",
                                                                self._device_uplink_messages_queue,
                                                                self._main_stop_event,
                                                                self._mqtt_manager,
                                                                self._device_rate_limiter,
                                                                self._gateway_rate_limiter)
        self._gateway_uplink_messages_queue: AsyncDeque = AsyncDeque(maxlen=max_queue_size)
        self._gateway_uplink_message_worker = MessageQueueWorker("GatewayUplinkMessageWorker",
                                                                 self._gateway_uplink_messages_queue,
                                                                 self._main_stop_event,
                                                                 self._mqtt_manager,
                                                                 self._device_rate_limiter,
                                                                 self._gateway_rate_limiter)
        self._active = asyncio.Event()
        self._wakeup_event = asyncio.Event()
        self._can_process_device_uplink_event = asyncio.Event()
        self._can_process_device_uplink_event.set()
        self._can_process_gateway_uplink_event = asyncio.Event()
        self._can_process_gateway_uplink_event.set()
        self._active.set()
        self._adapter = message_adapter
        self._gateway_adapter = gateway_message_adapter

        self._retry_by_qos_task = asyncio.create_task(self._dispatch_retry_by_qos_queue_loop())
        self._initial_queue_task = asyncio.create_task(self._dispatch_initial_queue_loop())
        self._service_queue_task = asyncio.create_task(self._dispatch_queue_loop(self._service_queue, self._service_message_worker))
        self._device_uplink_messages_queue_task = asyncio.create_task(self._dispatch_queue_loop(self._device_uplink_messages_queue, self._device_uplink_message_worker))
        self._gateway_uplink_messages_queue_task = asyncio.create_task(self._dispatch_queue_loop(self._gateway_uplink_messages_queue, self._gateway_uplink_message_worker))

        self._rate_limit_refill_task = asyncio.create_task(self._rate_limit_refill_loop())
        self.__print_queue_statistics_task = asyncio.create_task(self.print_queues_statistics())

    async def publish(self, message: MqttPublishMessage) -> Optional[List[asyncio.Future[PublishResult]]]:
        """
        Publish a message to the message queue.
        :param message: The MqttPublishMessage to publish.
        :return: A list of futures for delivery results.
        """
        try:
            if logger.isEnabledFor(TRACE_LEVEL):
                logger.trace(f"Pushing message to queue with delivery futures: {[f.uuid for f in message.delivery_futures]}")
            await self._initial_queue.put(message)
        except Exception as e:
            logger.error("Failed to push message to queue: %s", e)
            for future in message.delivery_futures:
                if future and not future.done():
                    future.set_result(PublishResult(message.topic, message.qos, -1, len(message.original_payload), -1))

    async def _dispatch_initial_queue_loop(self):
        """
        Loop to process messages from the initial queue and dispatch them to the appropriate queues.
        """
        while not self._main_stop_event.is_set() and self._active.is_set():
            try:
                if not self._mqtt_manager.is_connected():
                    await asyncio.sleep(self._QUEUE_COOLDOWN)
                    continue
                peeked_batch = await self._initial_queue.peek_batch(self._batch_max_count)

                gateway_messages = []
                device_messages = []
                for message in peeked_batch:
                    if isinstance(message.original_payload, bytes):
                        # If the message is a raw bytes payload, put it directly into the service queue
                        await self._service_queue.put(message)
                    elif isinstance(message.original_payload, GatewayUplinkMessage):
                        # If the message is a GatewayUplinkMessage, process it with the gateway adapter
                        gateway_messages.append(message)
                    elif isinstance(message.original_payload, DeviceUplinkMessage):
                        # If the message is a DeviceUplinkMessage, process it with the device adapter
                        device_messages.append(message)
                    else:
                        logger.warning("Unknown message type in initial queue: %s", type(message.original_payload))

                if gateway_messages:
                    # Process gateway messages in batches
                    messages = self._gateway_adapter.build_uplink_messages(gateway_messages)
                    await self._gateway_uplink_messages_queue.extend(messages)

                if device_messages:
                    # Process device messages in batches
                    messages = self._adapter.build_uplink_messages(device_messages)
                    await self._device_uplink_messages_queue.extend(messages)

                await self._initial_queue.pop_n(len(peeked_batch))

                if self._initial_queue.is_empty():
                    await asyncio.sleep(self._QUEUE_COOLDOWN)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Dispatch loop error: %s", e)
                await asyncio.sleep(0.5)

    async def _dispatch_queue_loop(self, queue: AsyncDeque, worker: 'MessageQueueWorker'):
        """Loop to process messages from the service queue."""
        while not self._main_stop_event.is_set() and self._active.is_set():
            message = None
            try:
                if not self._mqtt_manager.is_connected():
                    await asyncio.sleep(self._QUEUE_COOLDOWN)
                    continue
                message = await queue.get()
                if not message:
                    await asyncio.sleep(0.01)
                    continue
                logger.trace("Processing message from queue: %s, message_id: %s, message payload: %s",
                            message.topic, message.message_id, message.original_payload)
                expected_duration, expected_tokens, triggered_rate_limit = await worker.process(message)
                if triggered_rate_limit:
                    logger.trace("Reinserting message to the front of the queue: %s, message payload: %s",
                                message.uuid, message.original_payload)
                    await queue.reinsert_front(message)
                    triggered_rate_limit.set_required_tokens(expected_duration, expected_tokens)
                    await triggered_rate_limit.required_tokens_ready.wait()

                if queue.is_empty():
                    await asyncio.sleep(self._QUEUE_COOLDOWN)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Service queue loop error: %s", e)
                if message:
                    await queue.reinsert_front(message)
                await asyncio.sleep(1)

    async def _dispatch_retry_by_qos_queue_loop(self):
        while not self._main_stop_event.is_set() and self._active.is_set():
            message = None
            try:
                if not self._mqtt_manager.is_connected():
                    await asyncio.sleep(self._QUEUE_COOLDOWN)
                    continue
                message = await self._retry_by_qos_queue.get()
                if not message:
                    await asyncio.sleep(self._QUEUE_COOLDOWN)
                    continue

                logger.trace("Retrying QoS message: %s", message)

                if isinstance(message.original_payload, bytes):
                    await self._service_queue.reinsert_front(message)
                elif isinstance(message.original_payload, GatewayUplinkMessage):
                    await self._gateway_uplink_messages_queue.reinsert_front(message)
                elif isinstance(message.original_payload, DeviceUplinkMessage):
                    await self._device_uplink_messages_queue.reinsert_front(message)
                else:
                    logger.warning("Unknown message type in retry queue: %s", type(message.original_payload))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Retry QoS dispatch error: %s", e)
                if message:
                    await self._retry_by_qos_queue.reinsert_front(message)
                await asyncio.sleep(0.5)

    async def shutdown(self):
        logger.debug("Shutting down MessageQueue...")
        self._active.clear()
        self._wakeup_event.set()

        self._retry_by_qos_task.cancel()
        self._initial_queue_task.cancel()
        self._service_queue_task.cancel()
        self._device_uplink_messages_queue_task.cancel()
        self._gateway_uplink_messages_queue_task.cancel()
        if self._rate_limit_refill_task:
            self._rate_limit_refill_task.cancel()
        self.__print_queue_statistics_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._retry_by_qos_task
            await self._initial_queue_task
            await self._rate_limit_refill_task
            await self.__print_queue_statistics_task

        await self.clear()

        logger.debug("MessageQueue shutdown complete, message queue size: %d",
                        self._initial_queue.size())

    @staticmethod
    async def _cancel_tasks(tasks: set[asyncio.Task]):
        for task in list(tasks):
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks, return_exceptions=True)
        tasks.clear()

    def is_empty(self):
        return self._initial_queue.is_empty()

    async def clear(self):
        logger.debug("Clearing message queue...")
        for queue in [self._initial_queue, self._service_queue,
                                    self._device_uplink_messages_queue, self._gateway_uplink_messages_queue]:
            while not queue.is_empty():
                message: MqttPublishMessage = await queue.get()
                for future in message.delivery_futures:
                    if future and not future.done():
                        future.set_result(PublishResult(
                            topic=message.topic,
                            qos=message.qos,
                            message_id=-1,
                            payload_size=message.payload_size if isinstance(message.payload, bytes) else message.payload.size,
                            reason_code=-1
                        ))
        logger.debug("Message queue cleared.")

    async def _rate_limit_refill_loop(self):
        try:
            while self._active.is_set():
                await asyncio.sleep(1.0)
                await self._refill_rate_limits()
                logger.trace("Rate limits refilled, state: %s", self._device_rate_limiter)
        except asyncio.CancelledError:
            pass

    async def _refill_rate_limits(self):
        for rl in self._device_rate_limiter.values():
            if rl:
                await rl.refill()
        if self._gateway_rate_limiter:
            for rl in self._gateway_rate_limiter.values():
                if rl:
                    await rl.refill()

    def set_gateway_message_adapter(self, message_adapter: GatewayMessageAdapter):
        self._gateway_adapter = message_adapter

    async def put_retry_message(self, message: MqttPublishMessage):
        await self._retry_by_qos_queue.put(message)

    async def print_queues_statistics(self):
        """
        Prints the current statistics of message queues.
        """
        while self._active.is_set() and not self._main_stop_event.is_set():
            retry_queue_size = self._retry_by_qos_queue.size()
            initial_queue_size = self._initial_queue.size()
            service_queue_size = self._service_queue.size()
            device_uplink_queue_size = self._device_uplink_messages_queue.size()
            gateway_uplink_queue_size = self._gateway_uplink_messages_queue.size()
            active = self._active.is_set()
            logger.info("MessageQueue Statistics: "
                        "Initial Queue Size: %d, "
                        "Service Queue Size: %d, "
                        "Device Uplink Queue Size: %d, "
                        "Gateway Uplink Queue Size: %d, "
                        "Retry Queue Size: %d, "
                        "Active: %s",
                        initial_queue_size,
                        service_queue_size,
                        device_uplink_queue_size,
                        gateway_uplink_queue_size,
                        retry_queue_size,
                        active)
            await asyncio.sleep(60)

class MessageQueueWorker:
    def __init__(self,
                 name,
                 queue: AsyncDeque,
                 stop_event: asyncio.Event,
                 mqtt_manager: MQTTManager,
                 device_rate_limiter: RateLimiter,
                 gateway_rate_limiter: Optional[RateLimiter] = None):
        self.name = name
        self._queue = queue
        self._stop_event = stop_event
        self._mqtt_manager = mqtt_manager
        self._device_rate_limiter = device_rate_limiter
        self._gateway_rate_limiter = gateway_rate_limiter

    async def process(self, message: MqttPublishMessage) -> Tuple[Optional[int], Optional[int], Optional[RateLimit]]:
        message_rate_limit, datapoints_rate_limit = self._get_rate_limits_for_message(message)
        if message_rate_limit.has_limit() or datapoints_rate_limit.has_limit():
            triggered_rate_limit_entry, expected_tokens, rate_limit = await self.check_rate_limits_for_message(datapoints_count=message.datapoints,
                                                                                                               message_rate_limit=message_rate_limit,
                                                                                                               datapoints_rate_limit=datapoints_rate_limit)

            if triggered_rate_limit_entry is not None:
                triggered_duration = triggered_rate_limit_entry[1]
                logger.debug("Rate limit %r per %r seconds hit by message with expected tokens: %r.",
                             triggered_rate_limit_entry[0], triggered_duration, expected_tokens)
                return triggered_duration, expected_tokens, rate_limit

            await self._consume_rate_limits_for_message(datapoints_count=message.datapoints,
                                                        message_rate_limit=message_rate_limit,
                                                        datapoints_rate_limit=datapoints_rate_limit)
        await self._mqtt_manager.publish(message)
        return None, None, None


    def _get_rate_limits_for_message(self, message: MqttPublishMessage) -> Tuple[RateLimit, RateLimit]:

        message_rate_limit = EMPTY_RATE_LIMIT
        datapoints_rate_limit = EMPTY_RATE_LIMIT

        if message.is_device_message:
            if message.is_service_message:
                message_rate_limit = self._device_rate_limiter.message_rate_limit
            else:
                message_rate_limit = self._device_rate_limiter.telemetry_message_rate_limit
                datapoints_rate_limit = self._device_rate_limiter.telemetry_datapoints_rate_limit

        else:
            if message.is_service_message:
                message_rate_limit = self._gateway_rate_limiter.message_rate_limit
            else:
                message_rate_limit = self._gateway_rate_limiter.telemetry_message_rate_limit
                datapoints_rate_limit = self._gateway_rate_limiter.telemetry_datapoints_rate_limit

        return message_rate_limit, datapoints_rate_limit

    @staticmethod
    async def check_rate_limits_for_message(datapoints_count: int,
                                      message_rate_limit: RateLimit,
                                      datapoints_rate_limit: RateLimit) -> Tuple[Union[Tuple[int, int], None], int, Optional[RateLimit]]:
        if message_rate_limit and message_rate_limit.has_limit():
            triggered_rate_limit_entry = await message_rate_limit.try_consume(1)
            if triggered_rate_limit_entry:
                return triggered_rate_limit_entry, 1, message_rate_limit
        if datapoints_rate_limit and datapoints_rate_limit.has_limit():
            triggered_rate_limit_entry = await datapoints_rate_limit.try_consume(datapoints_count)
            if triggered_rate_limit_entry:
                return triggered_rate_limit_entry, datapoints_count, datapoints_rate_limit
        return None, 0, None

    @staticmethod
    async def _consume_rate_limits_for_message(datapoints_count: int,
                                      message_rate_limit: RateLimit,
                                      datapoints_rate_limit: RateLimit) -> None:
        if message_rate_limit and message_rate_limit.has_limit():
            await message_rate_limit.consume(1)
        if datapoints_rate_limit and datapoints_rate_limit.has_limit():
            await datapoints_rate_limit.consume(datapoints_count)
