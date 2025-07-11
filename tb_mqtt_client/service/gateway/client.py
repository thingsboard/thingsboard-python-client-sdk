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

from asyncio import sleep
from time import monotonic
from typing import Optional, Dict, Union

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.event_dispatcher import EventDispatcher
from tb_mqtt_client.service.gateway.gateway_client_interface import GatewayClientInterface
from tb_mqtt_client.service.gateway.handlers.gateway_attribute_updates_handler import GatewayAttributeUpdatesHandler
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter, JsonGatewayMessageAdapter

logger = get_logger(__name__)


class GatewayClient(DeviceClient, GatewayClientInterface):
    """
    ThingsBoard Gateway MQTT client implementation.
    This class extends DeviceClient and adds gateway-specific functionality.
    """
    SUBSCRIPTIONS_TIMEOUT = 1.0  # Timeout for subscribe/unsubscribe operations

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
        self._gateway_rpc_handler = None  # Placeholder for gateway RPC handler
        self._gateway_attribute_updates_handler = GatewayAttributeUpdatesHandler(self._event_dispatcher,
                                                                                 self._gateway_message_adapter,
                                                                                 self._device_manager)
        self._gateway_requested_attribute_response_handler = None  # Placeholder for gateway requested attribute response handler

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

        # Subscribe to gateway-specific topics
        await self._subscribe_to_gateway_topics()

        logger.info("Gateway connected to ThingsBoard.")

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
