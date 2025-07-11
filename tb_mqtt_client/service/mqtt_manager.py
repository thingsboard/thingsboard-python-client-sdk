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
from asyncio import sleep
from contextlib import suppress
from time import monotonic
from typing import Optional, Callable, Dict, Union, Tuple, Coroutine, Any

from gmqtt import Client as GMQTTClient, Message, Subscription

from tb_mqtt_client.common.async_utils import await_or_stop
from tb_mqtt_client.common.gmqtt_patch import patch_gmqtt_puback, patch_gmqtt_protocol_connection_lost, \
    patch_mqtt_handler_disconnect, patch_handle_connack, PatchUtils
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.rate_limit.backpressure_controller import BackpressureController
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.common.request_id_generator import RPCRequestIdProducer, AttributeRequestIdProducer
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.constants.service_keys import MESSAGES_RATE_LIMIT, TELEMETRY_MESSAGE_RATE_LIMIT, \
    TELEMETRY_DATAPOINTS_RATE_LIMIT
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.service.device.handlers.rpc_response_handler import RPCResponseHandler
from tb_mqtt_client.service.device.message_adapter import MessageAdapter

logger = get_logger(__name__)

QUOTA_EXCEEDED = 0x97  # MQTT 5 reason code (151)
IMPLEMENTATION_SPECIFIC_ERROR = 0x83  # MQTT 5 reason code (131)


class MQTTManager:

    _PUBLISH_TIMEOUT = 10.0  # Default timeout for publish operations

    def __init__(
        self,
        client_id: str,
        main_stop_event: asyncio.Event,
        message_adapter: MessageAdapter,
        on_connect: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        on_disconnect: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        on_publish_result: Optional[Callable[[PublishResult], Coroutine[Any, Any, None]]] = None,
        rate_limits_handler: Optional[Callable[[RPCResponse], Coroutine[Any, Any, None]]] = None,
        rpc_response_handler: Optional[RPCResponseHandler] = None,
    ):
        self._main_stop_event = main_stop_event
        self._message_dispatcher = message_adapter
        patch_gmqtt_protocol_connection_lost()
        patch_mqtt_handler_disconnect()

        self._client = GMQTTClient(client_id)
        patch_gmqtt_puback(self._client, self._handle_puback_reason_code)
        patch_handle_connack(self._client, self._on_connect_internal)
        self._client.on_connect = self._on_connect_internal
        self._client.on_disconnect = self._on_disconnect_internal
        self._client.on_message = self._on_message_internal
        self._client.on_publish = self._on_publish_internal
        self._client.on_subscribe = self._on_subscribe_internal
        self._client.on_unsubscribe = self._on_unsubscribe_internal

        self._on_connect_callback = on_connect
        self._on_disconnect_callback = on_disconnect
        self._on_publish_result_callback = on_publish_result

        self._connected_event = asyncio.Event()
        self._connect_params = None  # Will be set in connect method
        self._handlers: Dict[str, Callable[[str, bytes], Coroutine[Any, Any, None]]] = {}

        self._pending_publishes: Dict[int, Tuple[asyncio.Future[PublishResult], str, int, int, float]] = {}
        self._publish_monitor_task = asyncio.create_task(self._monitor_ack_timeouts())

        self._pending_subscriptions: Dict[int, asyncio.Future] = {}
        self._pending_unsubscriptions: Dict[int, asyncio.Future] = {}
        self._rpc_response_handler = rpc_response_handler or RPCResponseHandler()
        self.register_handler(mqtt_topics.DEVICE_RPC_RESPONSE_TOPIC_FOR_SUBSCRIPTION, self._rpc_response_handler.handle)

        self._backpressure = BackpressureController(self._main_stop_event)
        self.__rate_limits_handler = rate_limits_handler
        self.__rate_limits_retrieved = False
        self.__rate_limiter: Optional[Dict[str, RateLimit]] = None
        self.__is_gateway = False  # TODO: determine if this is a gateway or not
        self.__is_waiting_for_rate_limits_publish = True  # Start with True to prevent publishing before rate limits are retrieved
        self._rate_limits_ready_event = asyncio.Event()
        self._claiming_future = None

    # TODO: In case of implementing for gateway may be better to use a handler, to discuss
    def register_claiming_future(self, future: asyncio.Future):
        """
        Register a future that will be set when the claiming process is complete.
        This is used to ensure that the MQTT client does not publish messages before the claiming process is done.
        """
        self._claiming_future = future

    async def connect(self, host: str, port: int = 1883, username: Optional[str] = None,
                      password: Optional[str] = None, tls: bool = False,
                      keepalive: int = 60, ssl_context: Optional[ssl.SSLContext] = None):
        self._connect_params = (host, port, username, password, tls, keepalive, ssl_context)
        asyncio.create_task(self._connect_loop())

    async def _connect_loop(self):
        host, port, username, password, tls, keepalive, ssl_context = self._connect_params
        retry_delay = 3

        while not self._client.is_connected and not self._main_stop_event.is_set():
            try:
                if username:
                    self._client.set_auth_credentials(username, password)

                if tls:
                    if ssl_context is None:
                        ssl_context = ssl.create_default_context()
                    await self._client.connect(host, port, ssl=ssl_context, keepalive=keepalive)
                else:
                    await self._client.connect(host, port, keepalive=keepalive)

                logger.info("MQTT connection initiated, waiting for on_connect...")
                await self._connected_event.wait()
                logger.info("MQTT connected.")
                break

            except Exception as e:
                logger.warning("Initial MQTT connection failed: %s. Retrying in %s seconds...", str(e), retry_delay)
                await asyncio.sleep(retry_delay)

    def is_connected(self) -> bool:
        return self._client.is_connected and self._connected_event.is_set() and self.__rate_limits_retrieved

    async def disconnect(self):
        try:
            await self._client.disconnect()
        except ConnectionResetError:
            logger.debug("Connection reset error during disconnect, ignoring.")
        except Exception as e:
            logger.error("Error during MQTT disconnect: %s", str(e))
        await asyncio.sleep(0.2)
        self._connected_event.clear()
        self.__rate_limits_retrieved = False
        self.__is_waiting_for_rate_limits_publish = True
        self._rate_limits_ready_event.clear()

    async def publish(self, message_or_topic: Union[str, Message],
                      payload: Optional[bytes] = None,
                      qos: int = 1,
                      retain: bool = False,
                      force=False) -> asyncio.Future:

        if not force:
            if not self.__rate_limits_retrieved and not self.__is_waiting_for_rate_limits_publish:
                raise RuntimeError("Cannot publish before rate limits are retrieved.")
            try:
                if not self._rate_limits_ready_event.is_set():
                    await await_or_stop(self._rate_limits_ready_event.wait(), self._main_stop_event, timeout=10)
            except asyncio.TimeoutError:
                raise RuntimeError("Timeout waiting for rate limits.")

        if not force and self._backpressure.should_pause():
            logger.trace("Backpressure active. Publishing suppressed.")
            raise RuntimeError("Publishing temporarily paused due to backpressure.")

        if isinstance(message_or_topic, Message):
            message = message_or_topic
        else:
            message = Message(message_or_topic, payload, qos=qos, retain=retain)

        mid, package = self._client._connection.publish(message)  # noqa

        future = asyncio.get_event_loop().create_future()
        if qos > 0:
            logger.trace("Publishing mid=%s, storing publish main future with id: %r", mid, id(future))
            self._pending_publishes[mid] = (future, message.topic, message.qos, message.payload_size, monotonic())
            self._client._persistent_storage.push_message_nowait(mid, package)  # noqa
        else:
            future.set_result(True)

        return future

    async def subscribe(self, topic: Union[str, Subscription], qos: int = 1) -> asyncio.Future:
        sub_future = asyncio.get_event_loop().create_future()
        subscription = Subscription(topic, qos=qos) if isinstance(topic, str) else topic

        if self.__rate_limiter:
            await self.__rate_limiter[MESSAGES_RATE_LIMIT].consume()
        mid = self._client._connection.subscribe([subscription])  # noqa
        self._pending_subscriptions[mid] = sub_future
        return sub_future

    async def unsubscribe(self, topic: str) -> asyncio.Future:
        unsubscribe_future = asyncio.get_event_loop().create_future()
        if self.__rate_limiter:
            await self.__rate_limiter[MESSAGES_RATE_LIMIT].consume()
        mid = self._client._connection.unsubscribe(topic)  # noqa
        self._pending_unsubscriptions[mid] = unsubscribe_future
        return unsubscribe_future

    def register_handler(self, topic_filter: str, handler: Callable[[str, bytes], Coroutine[Any, Any, None]]):
        self._handlers[topic_filter] = handler

    def unregister_handler(self, topic_filter: str):
        self._handlers.pop(topic_filter, None)

    def _on_connect_internal(self, client, session_present, reason_code, properties):
        if reason_code != 0:
            logger.error("Failed to connect to platform with reason code: %s", reason_code)
            if properties and 'reason_string' in properties:
                logger.error("Connection reason: %s", properties['reason_string'][0])
            self._connected_event.clear()
            return
        logger.info("Connected to the platform.")
        logger.debug("Connection session_present: %s, reason code: %s, properties: %s", session_present, reason_code, properties)
        if hasattr(client, '_connection'):
            client._connection._on_disconnect_called = False  # noqa
        self._connected_event.set()
        asyncio.create_task(self.__handle_connect_and_limits())

    async def __handle_connect_and_limits(self):
        logger.debug("Subscribing to RPC response topics")
        sub_future = await self.subscribe(mqtt_topics.DEVICE_RPC_REQUEST_TOPIC_FOR_SUBSCRIPTION, qos=1)
        while not sub_future.done():
            await sleep(0.01)
            if self._main_stop_event.is_set():
                return
        sub_future = await self.subscribe(mqtt_topics.DEVICE_RPC_RESPONSE_TOPIC_FOR_SUBSCRIPTION, qos=1)
        while not sub_future.done():
            await sleep(0.01)
            if self._main_stop_event.is_set():
                return
        logger.debug("Subscribing completed, sending rate limits request")

        await self.__request_rate_limits()

        if self._on_connect_callback:
            asyncio.create_task(self._on_connect_callback())

    def _on_disconnect_internal(self, client, reason_code=None, properties=None, exc=None):  # noqa
        if isinstance(reason_code, bytes):
            # Skipping handling due to duplication, because gmqtt triggers this cb again.
            logger.trace("Received bytes reason code: %r", reason_code)
            return
        self._connected_event.clear()
        if reason_code is not None:
            reason_desc = PatchUtils.DISCONNECT_REASON_CODES.get(reason_code, "Unknown reason")
            logger.info("Disconnected from platform with reason code: %s (%s)", reason_code, reason_desc)

            if properties and 'reason_string' in properties:
                logger.info("Disconnect reason: %s", properties['reason_string'][0])
        else:
            logger.info("Disconnected from the platform.")

        if exc:
            logger.warning("Disconnect exception: %s", exc)

        for mid, (future, topic, qos, payload_size, publishing_time) in list(self._pending_publishes.items()):
            if not future.done():
                publish_result = PublishResult(
                    topic=topic,
                    qos=qos,
                    payload_size=payload_size,
                    message_id=-1,
                    reason_code=reason_code or 0
                )
                future.set_result(publish_result)
                logger.warning("Setting publish result for mid=%s: %r", mid, publish_result)
        self._pending_publishes.clear()

        RPCRequestIdProducer.reset()
        AttributeRequestIdProducer.reset()
        self._rpc_response_handler.clear()
        self._handlers.clear()
        self.__rate_limits_retrieved = False
        self.__is_waiting_for_rate_limits_publish = True
        self._rate_limits_ready_event.clear()
        if reason_code == 142:
            logger.error("Session was taken over, looks like another client connected with the same credentials.")
            self._backpressure.notify_disconnect(delay_seconds=10)
        if reason_code in (131, 142, 143, 151):
            reached_time = 1
            for rate_limit in self.__rate_limiter.values():
                if isinstance(rate_limit, RateLimit):
                    reached_limit = asyncio.get_event_loop().run_until_complete(rate_limit.reach_limit())
                    reached_index, reached_time, reached_duration = reached_limit if reached_limit else (None, None, 1)
            self._backpressure.notify_disconnect(delay_seconds=reached_time)
        elif reason_code != 0:
            # Default disconnect handling
            self._backpressure.notify_disconnect(delay_seconds=15)

        if self._on_disconnect_callback:
            asyncio.create_task(self._on_disconnect_callback())

    def _on_message_internal(self, client, topic: str, payload: bytes, qos, properties):
        logger.trace("Received message by client %r on topic %s with payload %r, qos %r, properties %r",
                     client, topic, payload, qos, properties)
        for topic_filter, handler in self._handlers.items():
            if self._match_topic(topic_filter, topic):
                asyncio.create_task(handler(topic, payload))
                return

    def _on_publish_internal(self, client, mid):
        logger.trace("Publish was sent by client %r with mid=%s", client, mid)

    def _handle_puback_reason_code(self, mid: int, reason_code: int, properties: dict):
        logger.trace("Handling PUBACK mid=%s with rc %r and properties: %r",
                     mid, reason_code, properties)
        pending_future_data = self._pending_publishes.pop(mid, None)
        if pending_future_data is None:
            logger.error("Missing future for mid=%s", mid)
            return
        future, topic, qos, payload_size, publishing_time = pending_future_data
        publish_result = PublishResult(
            topic=topic,
            qos=qos,
            payload_size=payload_size,
            message_id=mid,
            reason_code=reason_code
        )
        logger.trace("Received result for publish future (id: %r): %r", id(future), publish_result)
        if not future.done():
            future.set_result(publish_result)
        else:
            logger.warning("Future (id: %r) for mid=%s was already done, skipping setting result",
                           id(future), mid)

        if reason_code == QUOTA_EXCEEDED:
            logger.warning("PUBACK received with QUOTA_EXCEEDED for mid=%s", mid)
            self._backpressure.notify_quota_exceeded(delay_seconds=10)
        elif reason_code == IMPLEMENTATION_SPECIFIC_ERROR:
            logger.warning("PUBACK received with IMPLEMENTATION_SPECIFIC_ERROR for mid=%s, treating as rate limit reached", mid)
            self._backpressure.notify_quota_exceeded(delay_seconds=15)  # Treat implementation specific error as quota exceeded
        elif reason_code != 0:
            logger.warning("PUBACK received with error code %s for mid=%s", reason_code, mid)

        if self._on_publish_result_callback:
            asyncio.create_task(self._on_publish_result_callback(publish_result))

    def _on_subscribe_internal(self, client, mid, qos, properties):
        logger.trace("Received SUBACK by client %r for mid=%s with qos %s, properties %s",
                     client, mid, qos, properties)
        future = self._pending_subscriptions.pop(mid, None)
        if future and not future.done():
            future.set_result(mid)

    def _on_unsubscribe_internal(self, client, mid, properties):
        logger.trace("Received UNSUBACK by client %r for mid=%s with properties %s", client, mid, properties)
        future = self._pending_unsubscriptions.pop(mid, None)
        if future and not future.done():
            future.set_result(mid)

    async def await_ready(self, timeout: float = 10.0):
        try:
            await await_or_stop(self._rate_limits_ready_event.wait(), self._main_stop_event, timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug("Waiting for rate limits timed out.")

    def set_rate_limits(
            self,
            message_rate_limit: Union[RateLimit, Dict[str, RateLimit]],
            telemetry_message_rate_limit: Optional[RateLimit],
            telemetry_dp_rate_limit: Optional[RateLimit]
    ):
        self.__rate_limiter = {
            MESSAGES_RATE_LIMIT: message_rate_limit,
            TELEMETRY_MESSAGE_RATE_LIMIT: telemetry_message_rate_limit,
            TELEMETRY_DATAPOINTS_RATE_LIMIT: telemetry_dp_rate_limit
        }
        self.__rate_limits_retrieved = True
        self.__is_waiting_for_rate_limits_publish = False
        self._rate_limits_ready_event.set()

    async def __request_rate_limits(self):
        self.__is_waiting_for_rate_limits_publish = True

        logger.debug("Publishing rate limits request to server...")

        request = await RPCRequest.build("getSessionLimits")
        topic, payload = self._message_dispatcher.build_rpc_request(request)
        response_future = self._rpc_response_handler.register_request(request.request_id, self.__rate_limits_handler)

        try:
            await self.publish(topic, payload, qos=1, force=True)
            await await_or_stop(response_future, self._main_stop_event, timeout=10)
            logger.info("Successfully processed rate limits.")
            self.__rate_limits_retrieved = True
            self.__is_waiting_for_rate_limits_publish = False
            self._rate_limits_ready_event.set()
        except asyncio.TimeoutError:
            logger.warning("Timeout while waiting for rate limits.")
            # Keep __is_waiting_for_rate_limits_publish as True to prevent publishing
            # until rate limits are retrieved

    @property
    def backpressure(self) -> BackpressureController:
        return self._backpressure

    @staticmethod
    def _match_topic(filter_expression: str, topic: str) -> bool:
        filter_parts = filter_expression.split('/')
        topic_parts = topic.split('/')

        for i, filter_part in enumerate(filter_parts):
            if filter_part == '#':
                return True
            if i >= len(topic_parts):
                return False
            if filter_part != '+' and filter_part != topic_parts[i]:
                return False

        return len(filter_parts) == len(topic_parts)

    async def _monitor_ack_timeouts(self):
        while not self._main_stop_event.is_set():
            now = monotonic()
            await self.check_pending_publishes(now)
            # TODO: Add logic to handle expired futures, for subscriptions, rpc responses, etc.
            await asyncio.sleep(0.1)
        await self.check_pending_publishes(monotonic())

    async def check_pending_publishes(self, time_to_check):
        expired = []
        for mid, (future, topic, qos, payload_size, timestamp) in list(self._pending_publishes.items()):
            if self._main_stop_event.is_set():
                with suppress(asyncio.CancelledError):
                    future.cancel()
                continue
            if time_to_check - timestamp > self._PUBLISH_TIMEOUT:
                if not future.done():
                    logger.warning("Publish timeout: mid=%s, topic=%s", mid, topic)
                    result = PublishResult(topic, qos, payload_size, mid, reason_code=408)
                    future.set_result(result)
                expired.append(mid)
        for mid in expired:
            self._pending_publishes.pop(mid, None)
