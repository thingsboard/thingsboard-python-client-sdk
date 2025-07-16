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
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessageBuilder
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.event_dispatcher import EventDispatcher
from tb_mqtt_client.service.gateway.gateway_client_interface import GatewayClientInterface
from tb_mqtt_client.service.gateway.handlers.gateway_attribute_updates_handler import GatewayAttributeUpdatesHandler
from tb_mqtt_client.service.gateway.handlers.gateway_requested_attributes_response_handler import \
    GatewayRequestedAttributeResponseHandler
from tb_mqtt_client.service.gateway.handlers.gateway_rpc_handler import GatewayRPCHandler
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter, JsonGatewayMessageAdapter

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

        self._device_manager = DeviceManager()
        self._event_dispatcher: EventDispatcher = EventDispatcher()
        self._gateway_message_adapter: GatewayMessageAdapter = JsonGatewayMessageAdapter()

        self._multiplex_dispatcher = None  # Placeholder for multiplex dispatcher, if needed
        self._gateway_rpc_handler = GatewayRPCHandler(event_dispatcher=self._event_dispatcher,
                                                      message_adapter=self._gateway_message_adapter,
                                                      device_manager=self._device_manager)
        self._gateway_attribute_updates_handler = GatewayAttributeUpdatesHandler(event_dispatcher=self._event_dispatcher,
                                                                                 message_adapter=self._gateway_message_adapter,
                                                                                 device_manager=self._device_manager)
        self._gateway_requested_attribute_response_handler = GatewayRequestedAttributeResponseHandler(event_dispatcher=self._event_dispatcher,
                                                                                                      message_adapter=self._gateway_message_adapter,
                                                                                                      device_manager=self._device_manager)

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

        # Subscribe to gateway-specific topics
        await self._subscribe_to_gateway_topics()

        logger.info("Gateway connected to ThingsBoard.")


    async def connect_device(self, device_name: str, device_profile: str, wait_for_publish=False) -> Tuple[DeviceSession, List[Union[PublishResult, Future[PublishResult]]]]:
        """
        Connect a device to the gateway.

        :param device_name: Name of the device to connect
        :param device_profile: Profile of the device
        :param wait_for_publish: Whether to wait for the publish result
        :return: Tuple containing the DeviceSession and an optional PublishResult or list of PublishResults
        """
        logger.info("Connecting device %s with profile %s", device_name, device_profile)
        device_session = self._device_manager.register(device_name, device_profile)
        device_connect_message = DeviceConnectMessage.build(device_name, device_profile)
        topic, payload = self._gateway_message_adapter.build_device_connect_message_payload(device_connect_message)
        futures = await self._message_queue.publish(
            topic=topic,
            payload=payload,
            datapoints_count=1,
            qos=self._config.qos
        )

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
                result = PublishResult(topic, self._config.qos, -1, len(payload), -1)
            results.append(result)

        return device_session, results[0] if len(results) == 1 else results

    async def disconnect_device(self,  device_session: DeviceSession, wait_for_publish: bool):
        pass

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
        topic = mqtt_topics.GATEWAY_TELEMETRY_TOPIC
        futures = await self._message_queue.publish(
            topic=topic,
            payload=message,
            datapoints_count=message.timeseries_datapoint_count(),
            qos=self._config.qos
        )

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
                result = PublishResult(topic, self._config.qos, -1, message.size, -1)
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
        topic = mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC
        futures = await self._message_queue.publish(
            topic=topic,
            payload=message,
            datapoints_count=message.attributes_datapoint_count(),
            qos=self._config.qos
        )
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
                result = PublishResult(topic, self._config.qos, -1, message.size, -1)
            results.append(result)
        return results[0] if len(results) == 1 else results


    async def send_device_attributes_request(self, device_session: DeviceSession, attributes: Union[AttributeRequest, GatewayAttributeRequest], wait_for_publish: bool):
        pass

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
