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

from asyncio import sleep, Future
from time import monotonic
from typing import Optional, Dict, Union, Tuple, List, Any

from tb_mqtt_client.common.async_utils import await_or_stop
from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit, DEFAULT_RATE_LIMIT_PERCENTAGE
from tb_mqtt_client.common.rate_limit.rate_limiter import RateLimiter
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.device_disconnect_message import DeviceDisconnectMessage
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_claim_request import GatewayClaimRequest
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.direct_event_dispatcher import DirectEventDispatcher
from tb_mqtt_client.service.gateway.gateway_client_interface import GatewayClientInterface
from tb_mqtt_client.service.gateway.handlers.gateway_attribute_updates_handler import GatewayAttributeUpdatesHandler
from tb_mqtt_client.service.gateway.handlers.gateway_requested_attributes_response_handler import \
    GatewayRequestedAttributeResponseHandler
from tb_mqtt_client.service.gateway.handlers.gateway_rpc_handler import GatewayRPCHandler
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter, JsonGatewayMessageAdapter
from tb_mqtt_client.service.gateway.message_sender import GatewayMessageSender

logger = get_logger(__name__)


class GatewayClient(DeviceClient, GatewayClientInterface):
    """
    ThingsBoard Gateway MQTT client implementation.
    This class extends DeviceClient and adds gateway-specific functionality.
    """
    SUBSCRIPTIONS_TIMEOUT = 1.0  # Timeout for subscribe/unsubscribe operations
    OPERATIONAL_TIMEOUT = 5.0  # Timeout for connection events

    def __init__(self, config: Optional[Union[GatewayConfig, Dict]] = None):
        """
        Initialize a new GatewayClient instance.

        :param config: Gateway configuration object or dictionary
        """
        self._config = config if isinstance(config, GatewayConfig) else GatewayConfig(config)
        super().__init__(self._config)
        self._mqtt_manager.enable_gateway_mode()

        self.device_manager = DeviceManager()

        self._event_dispatcher: DirectEventDispatcher = DirectEventDispatcher()
        self._uplink_message_sender = GatewayMessageSender()
        self._event_dispatcher.register(GatewayEventType.DEVICE_CONNECT, self._uplink_message_sender.send_device_connect)
        self._event_dispatcher.register(GatewayEventType.DEVICE_DISCONNECT, self._uplink_message_sender.send_device_disconnect)
        self._event_dispatcher.register(GatewayEventType.DEVICE_UPLINK, self._uplink_message_sender.send_uplink_message)
        self._event_dispatcher.register(GatewayEventType.DEVICE_ATTRIBUTE_REQUEST, self._uplink_message_sender.send_attributes_request)
        self._event_dispatcher.register(GatewayEventType.DEVICE_RPC_RESPONSE, self._uplink_message_sender.send_rpc_response)
        self._event_dispatcher.register(GatewayEventType.GATEWAY_CLAIM_REQUEST, self._uplink_message_sender.send_claim_request)

        self._gateway_message_adapter: GatewayMessageAdapter = JsonGatewayMessageAdapter(1000, 1)  # Default max payload size and datapoints count limit, should be changed after connection established
        self._uplink_message_sender.set_message_adapter(self._gateway_message_adapter)

        self._multiplex_dispatcher = None  # Placeholder for multiplex dispatcher, if needed
        self._gateway_rpc_handler = GatewayRPCHandler(event_dispatcher=self._event_dispatcher,
                                                      message_adapter=self._gateway_message_adapter,
                                                      device_manager=self.device_manager,
                                                      stop_event=self._stop_event)
        self._gateway_attribute_updates_handler = GatewayAttributeUpdatesHandler(event_dispatcher=self._event_dispatcher,
                                                                                 message_adapter=self._gateway_message_adapter,
                                                                                 device_manager=self.device_manager)
        self._gateway_requested_attribute_response_handler = GatewayRequestedAttributeResponseHandler(event_dispatcher=self._event_dispatcher,
                                                                                                      message_adapter=self._gateway_message_adapter,
                                                                                                      device_manager=self.device_manager)

        # Gateway-specific rate limits
        self._device_messages_rate_limit = RateLimit("10:1,", name="device_messages")
        self._device_telemetry_rate_limit = RateLimit("10:1,", name="device_telemetry")
        self._device_telemetry_dp_rate_limit = RateLimit("10:1,", name="device_telemetry_datapoints")

        # Callbacks
        self._device_attribute_update_callback = None
        self._device_rpc_request_callback = None
        self._device_disconnect_callback = None

    async def connect(self):
        """
        Connect to the platform.
        """
        logger.info("Connecting gateway to platform at %s:%s", self._host, self._port)
        await super().connect()
        self._message_queue.set_gateway_message_adapter(self._gateway_message_adapter)
        self._uplink_message_sender.set_message_queue(self._message_queue)

        # Subscribe to gateway-specific topics
        await self._subscribe_to_gateway_topics()

        logger.info("Gateway connected to ThingsBoard.")


    async def connect_device(self,
                             device_name_or_device_connect_message: Union[str, DeviceConnectMessage],
                             device_profile: str = 'default',
                             wait_for_publish=False) -> Tuple[DeviceSession, List[Union[PublishResult, Future[PublishResult]]]]:
        """
        Connect a device to the gateway.

        :param device_name_or_device_connect_message: Name of the device or a DeviceConnectMessage object
        :param device_profile: Profile of the device
        :param wait_for_publish: Whether to wait for the publish result
        :return: Tuple containing the DeviceSession and an optional PublishResult or list of PublishResults
        """
        if not isinstance(device_name_or_device_connect_message, DeviceConnectMessage):
            device_name = device_name_or_device_connect_message
            device_connect_message = DeviceConnectMessage.build(device_name, device_profile)
        else:
            device_connect_message = device_name_or_device_connect_message

        logger.info("Connecting device %s with profile %s",
                    device_connect_message.device_name,
                    device_connect_message.device_profile)
        device_session = self.device_manager.register(device_connect_message.device_name,
                                                      device_connect_message.device_profile)
        futures = await self._event_dispatcher.dispatch(device_connect_message, qos=self._config.qos)  # noqa

        if not futures:
            logger.warning("No publish futures were returned from message queue")
            return device_session, []

        if not wait_for_publish:
            return device_session, futures

        results = []
        for fut in futures:
            try:
                result = await await_or_stop(fut, timeout=self.OPERATIONAL_TIMEOUT, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for telemetry publish result")
                result = PublishResult(mqtt_topics.GATEWAY_CONNECT_TOPIC, self._config.qos, -1, -1, -1)
            results.append(result)

        return device_session, results[0] if len(results) == 1 else results

    async def disconnect_device(self, device_session: DeviceSession, wait_for_publish: bool):
        """
        Disconnect a device from the gateway.

        :param device_session: The DeviceSession object for the device
        :param wait_for_publish: Whether to wait for the publish result
        :return: PublishResult or Future[PublishResult] if successful, None if failed
        """
        logger.info("Disconnecting device %s", device_session.device_info.device_name)
        if not device_session:
            logger.warning("No device session provided for disconnecting")
            return None
        device_disconnect_message = DeviceDisconnectMessage.build(device_session.device_info.device_name)

        futures = await self._event_dispatcher.dispatch(device_disconnect_message, qos=self._config.qos)  # noqa

        if not futures:
            logger.warning("No publish futures were returned from message queue")
            return device_session, []

        if not wait_for_publish:
            return device_session, futures

        results = []
        for fut in futures:
            try:
                result = await await_or_stop(fut, timeout=self.OPERATIONAL_TIMEOUT, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for telemetry publish result")
                result = PublishResult(mqtt_topics.GATEWAY_CONNECT_TOPIC, self._config.qos, -1, -1, -1)
            results.append(result)

        self.device_manager.unregister(device_session.device_info.device_id)
        return device_session, results[0] if len(results) == 1 else results

    async def send_device_timeseries(self,
                                     device_session: DeviceSession,
                                     data: Union[TimeseriesEntry, List[TimeseriesEntry], Dict[str, Any], List[Dict[str, Any]]],
                                     wait_for_publish: bool) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Send timeseries data to the platform for a specific device.
        :param device_session: The DeviceSession object for the device
        :param data: Timeseries data to send, can be a single entry or a list of entries
        :param wait_for_publish: Whether to wait for the publish result
        :return: List of PublishResults or Future objects, or None if no data was sent
        """
        logger.trace("Sending timeseries data for device %s", device_session.device_info.device_name)

        if not device_session or not data:
            logger.warning("No device session or data provided for sending timeseries")
            return None

        message = self._build_uplink_message_for_telemetry(data, device_session)
        futures = await self._event_dispatcher.dispatch(message, qos=self._config.qos)  # noqa

        if not futures:
            logger.warning("No publish futures were returned from message queue")
            return None

        if not wait_for_publish:
            return futures[0] if len(futures) == 1 else futures

        results = []
        for fut in futures:
            try:
                result = await await_or_stop(fut, timeout=self.OPERATIONAL_TIMEOUT, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for telemetry publish result")
                result = PublishResult(mqtt_topics.GATEWAY_TELEMETRY_TOPIC, self._config.qos, -1, message.size, -1)
            results.append(result)

        return results[0] if len(results) == 1 else results


    async def send_device_attributes(self,
                                     device_session: DeviceSession,
                                     data: Union[Dict[str, Any], AttributeEntry, list[AttributeEntry]],
                                     wait_for_publish: bool) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Send attributes data to the platform for a specific device.
        :param device_session: The DeviceSession object for the device
        :param data: Attributes data to send, can be a single entry or a list of entries
        :param wait_for_publish: Whether to wait for the publish result
        """
        logger.trace("Sending attributes data for device %s", device_session.device_info.device_name)
        if not device_session or not data:
            logger.warning("No device session or data provided for sending attributes")
            return None

        message = self._build_uplink_message_for_attributes(data, device_session)
        futures = await self._event_dispatcher.dispatch(message, qos=self._config.qos)  # noqa
        if not futures:
            logger.warning("No publish futures were returned from message queue")
            return None
        if not wait_for_publish:
            return futures[0] if len(futures) == 1 else futures
        results = []
        for fut in futures:
            try:
                result = await await_or_stop(fut, timeout=self.OPERATIONAL_TIMEOUT, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for attributes publish result")
                result = PublishResult(mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC, self._config.qos, -1, message.size, -1)
            results.append(result)
        return results[0] if len(results) == 1 else results


    async def send_device_attributes_request(self, device_session: DeviceSession, attribute_request: Union[AttributeRequest, GatewayAttributeRequest], wait_for_publish: bool):
        """
        Send a request for device attributes to the platform.
        :param device_session: The DeviceSession object for the device
        :param attribute_request: Attributes to request, can be a single AttributeRequest or GatewayAttributeRequest
        :param wait_for_publish: Whether to wait for the publish result
        """
        logger.trace("Sending attributes request for device %s", device_session.device_info.device_name)
        if not device_session or not attribute_request:
            logger.warning("No device session or attributes provided for sending attributes request")
            return None
        if isinstance(attribute_request, AttributeRequest):
            attribute_request = await GatewayAttributeRequest.from_attribute_request(device_session=device_session, attribute_request=attribute_request)

        await self._gateway_requested_attribute_response_handler.register_request(attribute_request)
        futures = await self._event_dispatcher.dispatch(attribute_request, qos=self._config.qos)

        if not futures:
            logger.warning("No publish futures were returned from message queue")
            return None

        if not wait_for_publish:
            return futures[0] if len(futures) == 1 else futures

        results = []
        for fut in futures:
            try:
                result = await await_or_stop(fut, timeout=self.OPERATIONAL_TIMEOUT, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for attributes request publish result")
                result = PublishResult(mqtt_topics.GATEWAY_ATTRIBUTES_REQUEST_TOPIC, self._config.qos, -1, -1, -1)
            results.append(result)

        return results[0] if len(results) == 1 else results

    async def send_device_claim_request(self,
                                        device_session: DeviceSession,
                                        gateway_claim_request: GatewayClaimRequest,
                                        wait_for_publish: bool) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Send a claim request for a device to the platform.
        :param device_session: The DeviceSession object for the device
        :param gateway_claim_request: Claim request data
        :param wait_for_publish: Whether to wait for the publish result
        """
        logger.trace("Sending claim request for device %s", device_session.device_info.device_name)
        if not device_session or not gateway_claim_request:
            logger.warning("No device session or claim request provided for sending claim request")
            return None

        futures = await self._event_dispatcher.dispatch(gateway_claim_request, qos=self._config.qos)  # noqa

        if not futures:
            logger.warning("No publish futures were returned from message queue")
            return None

        if not wait_for_publish:
            return futures[0] if len(futures) == 1 else futures

        results = []
        for fut in futures:
            try:
                result = await await_or_stop(fut, timeout=self.OPERATIONAL_TIMEOUT, stop_event=self._stop_event)
            except TimeoutError:
                logger.warning("Timeout while waiting for claim request publish result")
                result = PublishResult(mqtt_topics.GATEWAY_CLAIM_TOPIC, self._config.qos, -1, -1, -1)
            results.append(result)

        return results[0] if len(results) == 1 else results

    async def disconnect(self):
        """
        Disconnect from the platform.
        """
        logger.info("Disconnecting gateway from platform at %s:%s", self._host, self._port)
        await self._unsubscribe_from_gateway_topics()
        await super().disconnect()
        logger.info("Gateway disconnected from ThingsBoard.")

    async def _subscribe_to_gateway_topics(self):
        """
        Subscribe to gateway-specific MQTT topics.
        """
        logger.info("Subscribing to gateway topics")

        sub_future = await self._mqtt_manager.subscribe(mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC, qos=1)
        while not sub_future.done():
            await sleep(0.01)

        sub_future = await self._mqtt_manager.subscribe(mqtt_topics.GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, qos=1)
        while not sub_future.done():
            await sleep(0.01)

        sub_future = await self._mqtt_manager.subscribe(mqtt_topics.GATEWAY_RPC_TOPIC, qos=1)
        while not sub_future.done():
            await sleep(0.01)

        self._mqtt_manager.register_handler(mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC, self._gateway_attribute_updates_handler.handle)
        self._mqtt_manager.register_handler(mqtt_topics.GATEWAY_RPC_TOPIC, self._gateway_rpc_handler.handle)
        self._mqtt_manager.register_handler(mqtt_topics.GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, self._gateway_requested_attribute_response_handler.handle)

    async def _unsubscribe_from_gateway_topics(self):
        """
        Unsubscribe from gateway-specific MQTT topics.
        """
        logger.info("Unsubscribing from gateway topics")

        unsub_future = await self._mqtt_manager.unsubscribe(mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC)
        unsubscribe_start_time = monotonic()
        while not unsub_future.done():
            if monotonic() - unsubscribe_start_time > self.SUBSCRIPTIONS_TIMEOUT:
                logger.warning("Unsubscribe from gateway attributes topic timed out")
                break
            await sleep(0.01)

        unsub_future = await self._mqtt_manager.unsubscribe(mqtt_topics.GATEWAY_ATTRIBUTES_RESPONSE_TOPIC)
        unsubscribe_start_time = monotonic()
        while not unsub_future.done():
            if monotonic() - unsubscribe_start_time > self.SUBSCRIPTIONS_TIMEOUT:
                logger.warning("Unsubscribe from gateway attribute responses topic timed out")
                break
            await sleep(0.01)

        unsub_future = await self._mqtt_manager.unsubscribe(mqtt_topics.GATEWAY_RPC_TOPIC)
        unsubscribe_start_time = monotonic()
        while not unsub_future.done():
            if monotonic() - unsubscribe_start_time > self.SUBSCRIPTIONS_TIMEOUT:
                logger.warning("Unsubscribe from gateway rpc topic timed out")
                break
            await sleep(0.01)

    async def _handle_rate_limit_response(self, response: RPCResponse):  # noqa
        parent_rate_limits_processing = await super()._handle_rate_limit_response(response)
        try:
            if not isinstance(response.result, dict) or 'gatewayRateLimits' not in response.result:
                logger.warning("Invalid gateway rate limit response: %r", response)
                return None

            gateway_rate_limits = response.result.get('gatewayRateLimits', {})

            await self._gateway_rate_limiter.message_rate_limit.set_limit(gateway_rate_limits.get('messages', '0:0,'), percentage=DEFAULT_RATE_LIMIT_PERCENTAGE)
            await self._gateway_rate_limiter.telemetry_message_rate_limit.set_limit(gateway_rate_limits.get('telemetryMessages', '0:0,'), percentage=DEFAULT_RATE_LIMIT_PERCENTAGE)
            await self._gateway_rate_limiter.telemetry_datapoints_rate_limit.set_limit(gateway_rate_limits.get('telemetryDataPoints', '0:0,'), percentage=DEFAULT_RATE_LIMIT_PERCENTAGE)

            self._gateway_message_adapter.splitter.max_payload_size = self.max_payload_size
            self._gateway_message_adapter.splitter.max_datapoints = self._device_telemetry_dp_rate_limit.minimal_limit

            self._mqtt_manager.set_gateway_rate_limits_received()
            return parent_rate_limits_processing

        except Exception as e:
            logger.exception("Failed to parse rate limits from server response: %s", e)
            return False
