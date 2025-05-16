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
import ssl
from asyncio import sleep
from typing import Optional, Callable, Awaitable, Dict, Union

from gmqtt import Client as GMQTTClient, Message, Subscription, MQTTConnectError

from tb_mqtt_client.common.request_id_generator import RPCRequestIdProducer
from tb_mqtt_client.common.gmqtt_patch import patch_gmqtt_puback
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.common.rate_limit.backpressure_controller import BackpressureController
from tb_mqtt_client.service.rpc_response_handler import RPCResponseHandler
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.constants.service_keys import MESSAGES_RATE_LIMIT, TELEMETRY_MESSAGE_RATE_LIMIT, \
    TELEMETRY_DATAPOINTS_RATE_LIMIT
from tb_mqtt_client.constants.service_messages import SESSION_LIMITS_REQUEST_MESSAGE

logger = get_logger(__name__)


class MQTTManager:
    def __init__(
        self,
        client_id: str,
        on_connect: Optional[Callable[[], Awaitable[None]]] = None,
        on_disconnect: Optional[Callable[[], Awaitable[None]]] = None,
        rate_limits_handler: Optional[Callable[[str, bytes], Awaitable[None]]] = None,
        rpc_response_handler: Optional[RPCResponseHandler] = None,
    ):
        self._client = GMQTTClient(client_id)
        patch_gmqtt_puback(self._client, self._handle_puback_reason_code)
        self._client.on_connect = self._on_connect_internal
        self._client.on_disconnect = self._on_disconnect_internal
        self._client.on_message = self._on_message_internal
        self._client.on_publish = self._on_publish_internal
        self._client.on_subscribe = self._on_subscribe_internal
        self._client.on_unsubscribe = self._on_unsubscribe_internal

        self._on_connect_callback = on_connect
        self._on_disconnect_callback = on_disconnect

        self._connected_event = asyncio.Event()
        self._handlers: Dict[str, Callable[[str, bytes], Awaitable[None]]] = {}

        self._pending_publishes: Dict[int, asyncio.Future] = {}
        self._pending_subscriptions: Dict[int, asyncio.Future] = {}
        self._pending_unsubscriptions: Dict[int, asyncio.Future] = {}
        self._rpc_response_handler = rpc_response_handler or RPCResponseHandler()

        self._backpressure = BackpressureController()
        self.__rate_limits_handler = rate_limits_handler
        self.__rate_limits_retrieved = False
        self.__rate_limiter: Optional[Dict[str, RateLimit]] = None
        self.__is_gateway = False  # TODO: determine if this is a gateway or not
        self.__is_waiting_for_rate_limits_publish = False
        self._rate_limits_ready_event = asyncio.Event()

    async def connect(self, host: str, port: int = 1883, username: Optional[str] = None,
                      password: Optional[str] = None, tls: bool = False,
                      keepalive: int = 60, ssl_context: Optional[ssl.SSLContext] = None):
        try:
            if username:
                self._client.set_auth_credentials(username, password)

            if tls:
                if ssl_context is None:
                    ssl_context = ssl.create_default_context()
                await self._client.connect(host, port, ssl=ssl_context, keepalive=keepalive)
            else:
                await self._client.connect(host, port, keepalive=keepalive)
            try:
                await asyncio.wait_for(self._connected_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for MQTT connection.")
                raise

        except MQTTConnectError as e:
            logger.warning("MQTT connection failed: %s", str(e))
            self._connected_event.clear()
        except Exception as e:
            logger.exception("Unhandled exception during MQTT connect: %s", e)
            raise

    def is_connected(self) -> bool:
        return self._client.is_connected and self._connected_event.is_set() and self.__rate_limits_retrieved

    async def disconnect(self):
        await self._client.disconnect()
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
                await asyncio.wait_for(self._rate_limits_ready_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                raise RuntimeError("Timeout waiting for rate limits.")

        if not force and self._backpressure.should_pause():
            logger.warning("Backpressure active. Publishing suppressed.")
            raise RuntimeError("Publishing temporarily paused due to backpressure.")

        if isinstance(message_or_topic, Message):
            message = message_or_topic
        else:
            message = Message(message_or_topic, payload, qos=qos, retain=retain)

        mid, package = self._client._connection.publish(message)

        future = asyncio.get_event_loop().create_future()
        if qos > 0:
            self._pending_publishes[mid] = future
            self._client._persistent_storage.push_message_nowait(mid, package)
        else:
            future.set_result(True)

        return future

    async def subscribe(self, topic: Union[str, Subscription], qos: int = 1) -> asyncio.Future:
        sub_future = asyncio.get_event_loop().create_future()
        subscription = Subscription(topic, qos=qos) if isinstance(topic, str) else topic

        if self.__rate_limiter:
            self.__rate_limiter[MESSAGES_RATE_LIMIT].consume()
        mid = self._client._connection.subscribe([subscription])
        self._pending_subscriptions[mid] = sub_future
        return sub_future

    async def unsubscribe(self, topic: str) -> asyncio.Future:
        unsubscribe_future = asyncio.get_event_loop().create_future()
        if self.__rate_limiter:
            self.__rate_limiter[MESSAGES_RATE_LIMIT].consume()
        mid = self._client._connection.unsubscribe(topic)
        self._pending_unsubscriptions[mid] = unsubscribe_future
        return unsubscribe_future

    def register_handler(self, topic_filter: str, handler: Callable[[str, bytes], Awaitable[None]]):
        self._handlers[topic_filter] = handler

    def unregister_handler(self, topic_filter: str):
        self._handlers.pop(topic_filter, None)

    def _on_connect_internal(self, client, flags, rc, properties):
        logger.info("Connected to platform")
        self._connected_event.set()
        asyncio.create_task(self.__handle_connect_and_limits())

    async def __handle_connect_and_limits(self):
        logger.debug("Subscribing to RPC response topics")
        sub_future = await self.subscribe(mqtt_topics.DEVICE_RPC_REQUEST_TOPIC_FOR_SUBSCRIPTION, qos=1)
        while not sub_future.done():
            await sleep(0.01)
        sub_future = await self.subscribe(mqtt_topics.DEVICE_RPC_RESPONSE_TOPIC_FOR_SUBSCRIPTION, qos=1)
        while not sub_future.done():
            await sleep(0.01)
        logger.debug("Subscribing completed, sending rate limits request")

        await self.__request_rate_limits()

        if self._on_connect_callback:
            await self._on_connect_callback()

    def _on_disconnect_internal(self, client, packet, exc=None):
        logger.warning("Disconnected from platform")
        RPCRequestIdProducer.reset()
        self._rpc_response_handler.clear()
        self._connected_event.clear()
        self._backpressure.notify_disconnect(delay_seconds=15)
        if self._on_disconnect_callback:
            asyncio.create_task(self._on_disconnect_callback())

    def _on_message_internal(self, client, topic: str, payload: bytes, qos, properties):
        for topic_filter, handler in self._handlers.items():
            if self._match_topic(topic_filter, topic):
                asyncio.create_task(handler(topic, payload))
                return
        if topic.startswith(mqtt_topics.DEVICE_RPC_RESPONSE_TOPIC):
            asyncio.create_task(self._rpc_response_handler.handle(topic, payload))

    def _on_publish_internal(self, client, mid):
        future = self._pending_publishes.pop(mid, None)
        if future and not future.done():
            future.set_result(True)

    def _handle_puback_reason_code(self, mid: int, reason_code: int, properties: dict):
        QUOTA_EXCEEDED = 0x97  # MQTT 5 reason code for quota exceeded
        if reason_code == QUOTA_EXCEEDED:
            logger.warning("PUBACK received with QUOTA_EXCEEDED for mid=%s", mid)
            self._backpressure.notify_quota_exceeded(delay_seconds=10)
        elif reason_code != 0:
            logger.warning("PUBACK received with error code %s for mid=%s", reason_code, mid)

    def _on_subscribe_internal(self, client, mid, qos, properties):
        future = self._pending_subscriptions.pop(mid, None)
        if future and not future.done():
            future.set_result(True)

    def _on_unsubscribe_internal(self, client, mid):
        future = self._pending_unsubscriptions.pop(mid, None)
        if future and not future.done():
            future.set_result(True)

    async def await_ready(self, timeout: float = 10.0):
        try:
            await asyncio.wait_for(self._rate_limits_ready_event.wait(), timeout=timeout)
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
        request_id = await RPCRequestIdProducer.get_next()
        request_topic = f"v1/devices/me/rpc/request/{request_id}"
        response_topic = f"v1/devices/me/rpc/response/{request_id}"

        logger.debug("Publishing rate limits request to: %s", request_topic)
        response_future = self._rpc_response_handler.register_request(request_id)

        async def _handler(topic: str, payload: bytes):
            try:
                if self.__rate_limits_handler:
                    await self.__rate_limits_handler(topic, payload)
                response_future.set_result(payload)
            except Exception as e:
                logger.exception("Error handling rate limits response: %s", e)
                response_future.set_exception(e)

        self.register_handler(response_topic, _handler)

        try:
            self.__is_waiting_for_rate_limits_publish = True
            logger.debug("Requesting rate limits via RPC...")
            await self.publish(request_topic, SESSION_LIMITS_REQUEST_MESSAGE, qos=1, force=True)
            await asyncio.wait_for(response_future, timeout=10)
            logger.info("Successfully processed rate limits.")
            self.__rate_limits_retrieved = True
            self.__is_waiting_for_rate_limits_publish = False
            self._rate_limits_ready_event.set()
        except asyncio.TimeoutError:
            logger.warning("Timeout while waiting for rate limits.")
        finally:
            self.unregister_handler(response_topic)
            self.__is_waiting_for_rate_limits_publish = False

    @property
    def backpressure(self) -> BackpressureController:
        return self._backpressure

    @staticmethod
    def _match_topic(filter: str, topic: str) -> bool:
        filter_parts = filter.split('/')
        topic_parts = topic.split('/')

        for i, filter_part in enumerate(filter_parts):
            if filter_part == '#':
                return True
            if i >= len(topic_parts):
                return False
            if filter_part != '+' and filter_part != topic_parts[i]:
                return False

        return len(filter_parts) == len(topic_parts)
