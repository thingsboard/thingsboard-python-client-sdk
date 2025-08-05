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

import ssl
from asyncio import sleep, wait_for, TimeoutError, Event, Future
from random import choices
from string import ascii_uppercase, digits
from typing import Callable, Awaitable, Optional, Dict, Any, Union, List

from tb_mqtt_client.common.async_utils import await_or_stop
from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.common.provisioning_client import ProvisioningClient
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit, DEFAULT_RATE_LIMIT_PERCENTAGE
from tb_mqtt_client.common.rate_limit.rate_limiter import RateLimiter
from tb_mqtt_client.common.request_id_generator import RPCRequestIdProducer
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.base_client import BaseClient
from tb_mqtt_client.service.device.firmware_updater import FirmwareUpdater
from tb_mqtt_client.service.device.handlers.attribute_updates_handler import AttributeUpdatesHandler
from tb_mqtt_client.service.device.handlers.requested_attributes_response_handler import \
    RequestedAttributeResponseHandler
from tb_mqtt_client.service.device.handlers.rpc_requests_handler import RPCRequestsHandler
from tb_mqtt_client.service.device.handlers.rpc_response_handler import RPCResponseHandler
from tb_mqtt_client.service.device.message_adapter import JsonMessageAdapter, MessageAdapter
from tb_mqtt_client.service.message_service import MessageService
from tb_mqtt_client.service.mqtt_manager import MQTTManager

logger = get_logger(__name__)


class DeviceClient(BaseClient):
    def __init__(self, config: Optional[Union[DeviceConfig, Dict]] = None):
        self._stop_event = Event()
        self._config = None
        if isinstance(config, DeviceConfig):
            self._config = config
        else:
            self._config = DeviceConfig()
        if isinstance(config, dict):
            for key, value in config.items():
                if hasattr(self._config, key) and value is not None:
                    setattr(self._config, key, value)

        client_id = self._config.client_id or "tb-client-" + ''.join(choices(ascii_uppercase + digits, k=6))

        super().__init__(self._config.host, self._config.port, client_id)

        self._message_queue: Optional[MessageService] = None
        self._message_adapter: MessageAdapter = JsonMessageAdapter(1000,
                                                                   1)  # Will be updated after connection established

        self._rate_limiter = RateLimiter(
            message_rate_limit=RateLimit("0:0,", name="messages"),
            telemetry_message_rate_limit=RateLimit("0:0,", name="telemetryMessages"),
            telemetry_datapoints_rate_limit=RateLimit("0:0,", name="telemetryDataPoints")
        )
        self._gateway_rate_limiter = RateLimiter(
            message_rate_limit=RateLimit("0:0,", name="messages"),
            telemetry_message_rate_limit=RateLimit("0:0,", name="telemetryMessages"),
            telemetry_datapoints_rate_limit=RateLimit("0:0,", name="telemetryDataPoints")
        )
        self._ssl_context = None
        self.max_payload_size = None
        self._max_inflight_messages = 100
        self._max_uplink_message_queue_size = 10000
        self._max_queued_messages = 50000

        self._rpc_response_handler = RPCResponseHandler()

        self._mqtt_manager = MQTTManager(client_id=self._client_id,
                                         main_stop_event=self._stop_event,
                                         message_adapter=self._message_adapter,
                                         on_connect=self._on_connect,
                                         on_disconnect=self._on_disconnect,
                                         on_publish_result=self.__on_publish_result,
                                         rate_limits_handler=self._handle_rate_limit_response,
                                         rpc_response_handler=self._rpc_response_handler)

        self._requested_attribute_response_handler = RequestedAttributeResponseHandler()
        self._attribute_updates_handler = AttributeUpdatesHandler()
        self._rpc_requests_handler = RPCRequestsHandler()

        self._firmware_updater = FirmwareUpdater(self)

    async def update_firmware(self, on_received_callback: Optional[Callable[[str], Awaitable[None]]] = None,
                              save_firmware: bool = True, firmware_save_path: Optional[str] = None):
        await self._firmware_updater.update(on_received_callback, save_firmware, firmware_save_path)

    async def connect(self):
        logger.info("Connecting to platform at %s:%s", self._host, self._port)

        tls = self._config.use_tls()
        if tls:
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.load_verify_locations(self._config.ca_cert)
            self._ssl_context.load_cert_chain(certfile=self._config.client_cert, keyfile=self._config.private_key)

        await self._mqtt_manager.connect(
            host=self._host,
            port=self._port,
            username=self._config.access_token or self._config.username,
            password=None if self._config.access_token else self._config.password,
            tls=tls,
            ssl_context=self._ssl_context
        )

        while not self._mqtt_manager.is_connected():
            await self._mqtt_manager.await_ready()
            if self._stop_event.is_set():
                return

        # Initialize with default max_payload_size if not set
        if self.max_payload_size is None:
            self.max_payload_size = 65535
            logger.debug("Using default max_payload_size: %d", self.max_payload_size)

        self._message_adapter = JsonMessageAdapter(self.max_payload_size,
                                                   self._rate_limiter.telemetry_datapoints_rate_limit.minimal_limit)
        self._message_queue = MessageService(
            mqtt_manager=self._mqtt_manager,
            main_stop_event=self._stop_event,
            device_rate_limiter=self._rate_limiter,
            gateway_rate_limiter=self._gateway_rate_limiter,
            message_adapter=self._message_adapter,
            max_queue_size=self._max_uplink_message_queue_size,
        )

        self._requested_attribute_response_handler.set_message_adapter(self._message_adapter)
        self._attribute_updates_handler.set_message_adapter(self._message_adapter)
        self._rpc_requests_handler.set_message_adapter(self._message_adapter)
        self._rpc_response_handler.set_message_adapter(self._message_adapter)

    async def stop(self):
        """
        Stops the client and disconnects from the MQTT broker.
        """
        logger.info("Stopping DeviceClient...")
        self._stop_event.set()

        for fut, _ in self._rpc_response_handler._pending_rpc_requests.values():
            if not fut.done():
                fut.cancel()

        if self._message_queue:
            await self._message_queue.shutdown()

        await self._mqtt_manager.stop()

        logger.info("DeviceClient stopped.")

    async def disconnect(self):
        await self._mqtt_manager.disconnect()

    async def send_telemetry(self, *args, **kwargs):
        """
        Note: This method is deprecated. Use `send_timeseries` instead.
        """
        logger.warning("send_telemetry is deprecated. Use send_timeseries instead.")
        return await self.send_timeseries(*args, **kwargs)

    async def send_timeseries(
            self,
            data: Union[TimeseriesEntry, List[TimeseriesEntry], Dict[str, Any], List[Dict[str, Any]]],
            wait_for_publish: bool = True,
            timeout: Optional[float] = None
    ) -> Optional[Union[PublishResult, List[PublishResult], Future[PublishResult], List[Future[PublishResult]]]]:
        """
        Sends timeseries data to the ThingsBoard server.
        :param data: Timeseries data to send, can be a single TimeseriesEntry, a list of TimeseriesEntries,
                     a dictionary of key-value pairs, or a list of dictionaries.
        :param wait_for_publish: If True, waits for the publish result.
        :param timeout: Timeout for waiting for the publish result.
        :return: PublishResult or list of PublishResults if wait_for_publish is True, Future or list of Futures if not,
                    None if no data is sent.
        """
        message = self._build_uplink_message_for_telemetry(data)
        mqtt_message = MqttPublishMessage(
            topic=mqtt_topics.DEVICE_TELEMETRY_TOPIC,
            payload=message,
            qos=self._config.qos,
            datapoints_count=message.timeseries_datapoint_count()
        )
        delivery_future = mqtt_message.delivery_futures

        await self._message_queue.publish(mqtt_message)

        if not wait_for_publish:
            return delivery_future

        if isinstance(delivery_future, list):
            delivery_future = delivery_future[0]

        logger.info("Delivery future id in device client.send_timeseries: %r", delivery_future.uuid)

        try:
            result = await await_or_stop(delivery_future, timeout=1, stop_event=self._stop_event)
        except TimeoutError:
            logger.warning("Timeout while waiting for telemetry publish result")
            result = PublishResult(mqtt_message.topic, self._config.qos, -1, message.size, -1)
        return result

    async def send_attributes(
            self,
            attributes: Union[Dict[str, Any], AttributeEntry, list[AttributeEntry]],
            wait_for_publish: bool = True,
            timeout: int = BaseClient.DEFAULT_TIMEOUT
    ) -> Optional[Union[PublishResult, List[PublishResult], Future[PublishResult], List[Future[PublishResult]]]]:
        message = self._build_uplink_message_for_attributes(attributes)
        mqtt_message = MqttPublishMessage(
            topic=mqtt_topics.DEVICE_ATTRIBUTES_TOPIC,
            payload=message,
            qos=self._config.qos,
            datapoints_count=message.attributes_datapoint_count()
        )

        await self._message_queue.publish(mqtt_message)

        futures = mqtt_message.delivery_futures

        if not futures:
            logger.warning("No publish futures were returned from message queue")
            return None

        if not wait_for_publish:
            return futures

        results = []
        for fut in futures:
            try:
                result = await await_or_stop(fut, timeout=timeout, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for attribute publish result")
                result = PublishResult(mqtt_message.topic, self._config.qos, -1, message.size, -1)
            results.append(result)

        return results[0] if len(results) == 1 else results

    async def send_rpc_request(
            self,
            rpc_request: RPCRequest,
            callback: Optional[Callable[[RPCResponse], Awaitable[None]]] = None,
            wait_for_publish: bool = True,
            timeout: Optional[float] = BaseClient.DEFAULT_TIMEOUT
    ) -> Optional[Union[RPCResponse, Awaitable[RPCResponse]]]:
        request_id = rpc_request.request_id or await RPCRequestIdProducer.get_next()
        message_to_send = self._message_adapter.build_rpc_request(rpc_request)
        message_to_send.qos = self._config.qos
        response_future = self._rpc_response_handler.register_request(request_id, callback)

        await self._message_queue.publish(message_to_send)

        if not wait_for_publish:
            return response_future

        try:
            return await await_or_stop(response_future, timeout=timeout, stop_event=self._stop_event)
        except TimeoutError as e:
            if not callback:
                raise TimeoutError(f"Timed out waiting for RPC response (requestId={request_id})")
            else:
                logger.warning("Timed out waiting for RPC response, but callback is set. "
                               "Callback will be called with None response.")
                await self._rpc_response_handler.handle(
                    mqtt_topics.build_device_rpc_response_topic(rpc_request.request_id), e)

    async def send_rpc_response(self, response: RPCResponse):
        mqtt_message = self._message_adapter.build_rpc_response(response)
        mqtt_message.qos = self._config.qos
        await self._message_queue.publish(mqtt_message)

        delivery_future = mqtt_message.delivery_futures

        if isinstance(delivery_future, list):
            delivery_future = delivery_future[0]
        try:
            return await await_or_stop(delivery_future, timeout=BaseClient.DEFAULT_TIMEOUT, stop_event=self._stop_event)
        except TimeoutError:
            logger.warning("Timeout while waiting for RPC response publish result")
            return PublishResult(mqtt_message.topic, mqtt_message.qos, -1, len(mqtt_message.payload), -1)

    async def send_attribute_request(self,
                                     attribute_request: AttributeRequest,
                                     callback: Callable[[RequestedAttributeResponse], Awaitable[None]]):
        await self._requested_attribute_response_handler.register_request(attribute_request, callback)

        mqtt_message = self._message_adapter.build_attribute_request(attribute_request)
        mqtt_message.qos = self._config.qos

        await self._message_queue.publish(mqtt_message)

    async def claim_device(self,
                           claim_request: ClaimRequest,
                           wait_for_publish: bool = True,
                           timeout: float = BaseClient.DEFAULT_TIMEOUT) -> Union[Future[PublishResult], PublishResult]:
        mqtt_message = self._message_adapter.build_claim_request(claim_request)
        mqtt_message.qos = self._config.qos

        delivery_future = mqtt_message.delivery_futures

        await self._message_queue.publish(mqtt_message)
        if isinstance(delivery_future, list):
            delivery_future = delivery_future[0]
        if wait_for_publish:
            try:
                return await await_or_stop(delivery_future, timeout=timeout, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for claiming publish result")
                return PublishResult(mqtt_message.topic, 1, -1, mqtt_message.payload_size, -1)
        else:
            return delivery_future

    def set_attribute_update_callback(self, callback: Callable[[AttributeUpdate], Awaitable[None]]):
        self._attribute_updates_handler.set_callback(callback)

    def set_rpc_request_callback(self, callback: Callable[[RPCRequest], Awaitable[RPCResponse]]):
        self._rpc_requests_handler.set_callback(callback)

    async def _on_connect(self):
        logger.info("Subscribing to attribute and RPC topics")

        sub_future = await self._mqtt_manager.subscribe(mqtt_topics.DEVICE_ATTRIBUTES_TOPIC, qos=1)
        while not sub_future.done():
            await sleep(0.01)
            if self._stop_event.is_set():
                return

        self._mqtt_manager.register_handler(mqtt_topics.DEVICE_ATTRIBUTES_TOPIC, self._handle_attribute_update)
        self._mqtt_manager.register_handler(mqtt_topics.DEVICE_RPC_REQUEST_TOPIC_FOR_SUBSCRIPTION,
                                            self._handle_rpc_request)  # noqa
        self._mqtt_manager.register_handler(mqtt_topics.DEVICE_ATTRIBUTES_RESPONSE_TOPIC,
                                            self._handle_requested_attribute_response)  # noqa
        # RPC responses are handled by the RPCResponseHandler, which is already registered

    async def _on_disconnect(self):
        logger.info("Device client disconnected.")
        self._requested_attribute_response_handler.clear()

    async def _handle_attribute_update(self, topic: str, payload: bytes):
        await self._attribute_updates_handler.handle(topic, payload)

    async def _handle_rpc_request(self, topic: str, payload: bytes):
        response: RPCResponse = await self._rpc_requests_handler.handle(topic, payload)
        if response:
            await self.send_rpc_response(response)

    async def _handle_rpc_response(self, topic: str, payload: bytes):
        await self._rpc_response_handler.handle(topic, payload)

    async def _handle_requested_attribute_response(self, topic: str, payload: bytes):
        await self._requested_attribute_response_handler.handle(topic, payload)

    async def _handle_rate_limit_response(self, response: RPCResponse):  # noqa
        try:
            logger.debug("Received rate limit response payload: %s", response)

            if not isinstance(response.result, dict) or 'rateLimits' not in response.result:
                logger.warning("Invalid rate limit response: %r", response)
                return None

            rate_limits = response.result.get('rateLimits', {})

            await self._rate_limiter.message_rate_limit.set_limit(rate_limits.get("messages", "0:0,"),
                                                                  percentage=DEFAULT_RATE_LIMIT_PERCENTAGE)
            await self._rate_limiter.telemetry_message_rate_limit.set_limit(
                rate_limits.get("telemetryMessages", "0:0,"), percentage=DEFAULT_RATE_LIMIT_PERCENTAGE)
            await self._rate_limiter.telemetry_datapoints_rate_limit.set_limit(
                rate_limits.get("telemetryDataPoints", "0:0,"), percentage=DEFAULT_RATE_LIMIT_PERCENTAGE)

            server_inflight = int(response.result.get("maxInflightMessages", 100))
            limits = [rl.minimal_limit for rl in [
                self._rate_limiter.message_rate_limit,
                self._rate_limiter.telemetry_message_rate_limit,
            ] if rl.has_limit()]

            if limits:
                self._max_inflight_messages = int(
                    min(min(limits), server_inflight) * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
            else:
                self._max_inflight_messages = int(server_inflight * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
                if self._max_inflight_messages == 0:
                    self._max_inflight_messages = 10000

            if "maxPayloadSize" in response.result:
                self.max_payload_size = int(response.result["maxPayloadSize"] * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
                # Update the adapter's splitter with the new max_payload_size
                if self._message_adapter is not None:
                    self._message_adapter.splitter.max_payload_size = self.max_payload_size
                    logger.debug("Updated adapter's max_payload_size to %d", self.max_payload_size)
            else:
                # If maxPayloadSize is not provided, keep the default value
                logger.debug("No maxPayloadSize in service config, using default: %d", self.max_payload_size)
                # Initialize with default max_payload_size if not set
                if self.max_payload_size is None:
                    self.max_payload_size = 65535
                    logger.debug("Using default max_payload_size: %d", self.max_payload_size)
                    # Update the dispatcher's max_payload_size if it's already initialized
                    if self._message_adapter is not None and hasattr(self._message_adapter, 'splitter'):
                        self._message_adapter.splitter.max_payload_size = self.max_payload_size
                        logger.debug("Updated dispatcher's max_payload_size to %d", self.max_payload_size)

            self._message_adapter.splitter.max_datapoints = self._rate_limiter.telemetry_datapoints_rate_limit.minimal_limit  # noqa

            if (not self._rate_limiter.message_rate_limit.has_limit()
                    and not self._rate_limiter.telemetry_message_rate_limit.has_limit()
                    and not self._rate_limiter.telemetry_datapoints_rate_limit.has_limit()):
                self._max_queued_messages = 50000
                logger.debug("No rate limits, setting max_queued_messages to 50000")
            else:
                self._max_queued_messages = self._max_inflight_messages
                logger.debug("With rate limits, setting max_queued_messages to %r", self._max_queued_messages)

            logger.info("Service configuration retrieved and applied.")
            logger.info("Parsed device limits: %r", response)

            self._mqtt_manager.set_rate_limits_received()
            return True

        except Exception as e:
            logger.exception("Failed to parse rate limits from server response: %s", e)
            return False

    async def __on_publish_result(self, publish_result: PublishResult):
        """
        Callback for handling publish results.
        This can be used to handle the result of a publish operation, such as logging or updating state.
        """
        if publish_result.is_successful():
            logger.trace("Publish successful: %r", publish_result)
        else:
            logger.error("Publish failed: %r", publish_result)

    @staticmethod
    async def provision(provision_request: 'ProvisioningRequest', timeout=BaseClient.DEFAULT_TIMEOUT):
        provision_client = ProvisioningClient(
            host=provision_request.host,
            port=provision_request.port,
            provision_request=provision_request
        )

        device_credentials = None
        try:
            device_credentials = await wait_for(provision_client.provision(), timeout=timeout)
        except TimeoutError:
            logger.error("Provisioning timed out")

        return device_credentials
