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

from asyncio import sleep
from typing import Callable, Awaitable, Optional, Dict, Any, Union, List, Set

from orjson import dumps, loads
from random import choices
from string import ascii_uppercase, digits

from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit
from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.client import DeviceClient

logger = get_logger(__name__)


class GatewayClient(DeviceClient):
    """
    ThingsBoard Gateway MQTT client implementation.
    This class extends DeviceClient and adds gateway-specific functionality.
    """

    def __init__(self, config: Optional[Union[GatewayConfig, Dict]] = None):
        """
        Initialize a new GatewayClient instance.

        :param config: Gateway configuration object or dictionary
        """
        self._config = None
        if isinstance(config, GatewayConfig):
            self._config = config
        else:
            self._config = GatewayConfig()
        if isinstance(config, dict):
            for key, value in config.items():
                if hasattr(self._config, key) and value is not None:
                    setattr(self._config, key, value)

        client_id = self._config.client_id or "tb-gateway-" + ''.join(choices(ascii_uppercase + digits, k=6))

        # Initialize the DeviceClient with the gateway configuration
        super().__init__(self._config)

        # Gateway-specific rate limits
        self._device_messages_rate_limit = RateLimit("0:0,", name="device_messages")
        self._device_telemetry_rate_limit = RateLimit("0:0,", name="device_telemetry")
        self._device_telemetry_dp_rate_limit = RateLimit("0:0,", name="device_telemetry_datapoints")

        # Set of connected devices
        self._connected_devices: Set[str] = set()

        # Callbacks
        self._device_attribute_update_callback = None
        self._device_rpc_request_callback = None
        self._device_disconnect_callback = None

    async def connect(self):
        """
        Connect to the ThingsBoard platform.
        """
        logger.info("Connecting gateway to platform at %s:%s", self._host, self._port)
        await super().connect()

        # Subscribe to gateway-specific topics
        await self._subscribe_to_gateway_topics()

        logger.info("Gateway connected to ThingsBoard.")

    async def _subscribe_to_gateway_topics(self):
        """
        Subscribe to gateway-specific MQTT topics.
        """
        logger.info("Subscribing to gateway topics")

        # Subscribe to gateway attributes topic
        sub_future = await self._mqtt_manager.subscribe(mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC, qos=1)
        while not sub_future.done():
            await sleep(0.01)

        # Subscribe to gateway attributes response topic
        sub_future = await self._mqtt_manager.subscribe(mqtt_topics.GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, qos=1)
        while not sub_future.done():
            await sleep(0.01)

        # Subscribe to gateway RPC topic
        sub_future = await self._mqtt_manager.subscribe(mqtt_topics.GATEWAY_RPC_TOPIC, qos=1)
        while not sub_future.done():
            await sleep(0.01)

        # Register handlers for gateway topics
        self._mqtt_manager.register_handler(mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC, self._handle_gateway_attribute_update)
        self._mqtt_manager.register_handler(mqtt_topics.GATEWAY_RPC_TOPIC, self._handle_gateway_rpc_request)
        self._mqtt_manager.register_handler(mqtt_topics.GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, self._handle_gateway_attribute_response)

    async def _handle_gateway_attribute_update(self, topic: str, payload: bytes):
        """
        Handle attribute updates for gateway devices.

        :param topic: MQTT topic
        :param payload: Message payload
        """
        try:
            data = loads(payload)
            logger.debug("Received gateway attribute update: %s", data)

            if self._device_attribute_update_callback:
                for device_name, attributes in data.items():
                    update = AttributeUpdate(device=device_name, attributes=attributes)
                    await self._device_attribute_update_callback(update)
        except Exception as e:
            logger.exception("Error handling gateway attribute update: %s", e)

    async def _handle_gateway_rpc_request(self, topic: str, payload: bytes):
        """
        Handle RPC requests for gateway devices.

        :param topic: MQTT topic
        :param payload: Message payload
        """
        try:
            data = loads(payload)
            logger.debug("Received gateway RPC request: %s", data)

            if self._device_rpc_request_callback and 'device' in data and 'data' in data:
                device_name = data['device']
                rpc_data = data['data']

                if 'id' in rpc_data and 'method' in rpc_data:
                    request_id = rpc_data['id']
                    method = rpc_data['method']
                    params = rpc_data.get('params', {})

                    result = await self._device_rpc_request_callback(device_name, method, params)

                    # Send RPC response
                    await self.gw_send_rpc_reply(device_name, request_id, result)
        except Exception as e:
            logger.exception("Error handling gateway RPC request: %s", e)

    async def _handle_gateway_attribute_response(self, topic: str, payload: bytes):
        """
        Handle attribute responses for gateway devices.

        :param topic: MQTT topic
        :param payload: Message payload
        """
        try:
            data = loads(payload)
            logger.debug("Received gateway attribute response: %s", data)

            # Process attribute response if needed
            # This is typically used for handling responses to attribute requests
        except Exception as e:
            logger.exception("Error handling gateway attribute response: %s", e)

    async def gw_connect_device(self, device_name: str):
        """
        Connect a device to the gateway.

        :param device_name: Name of the device to connect
        """
        if device_name in self._connected_devices:
            logger.warning("Device %s is already connected", device_name)
            return

        self._connected_devices.add(device_name)
        logger.info("Device %s connected to gateway", device_name)

    async def gw_disconnect_device(self, device_name: str):
        """
        Disconnect a device from the gateway.

        :param device_name: Name of the device to disconnect
        """
        if device_name not in self._connected_devices:
            logger.warning("Device %s is not connected", device_name)
            return

        self._connected_devices.remove(device_name)

        # Publish device disconnect message
        await self._mqtt_manager.publish(
            mqtt_topics.GATEWAY_DISCONNECT_TOPIC,
            dumps({"device": device_name}),
            qos=1
        )

        logger.info("Device %s disconnected from gateway", device_name)

        # Call disconnect callback if registered
        if self._device_disconnect_callback:
            await self._device_disconnect_callback(device_name)

    async def gw_send_telemetry(self, device_name: str, telemetry: Union[Dict[str, Any], TimeseriesEntry, List[TimeseriesEntry]]):
        """
        Send telemetry on behalf of a connected device.

        :param device_name: Name of the device
        :param telemetry: Telemetry data to send
        """
        if device_name not in self._connected_devices:
            logger.warning("Cannot send telemetry for disconnected device %s", device_name)
            return

        # Convert telemetry to the appropriate format
        payload = self._prepare_telemetry_payload(device_name, telemetry)

        # Publish telemetry
        await self._mqtt_manager.publish(
            mqtt_topics.GATEWAY_TELEMETRY_TOPIC,
            dumps(payload),
            qos=1
        )

        logger.debug("Sent telemetry for device %s: %s", device_name, payload)

    async def gw_send_attributes(self, device_name: str, attributes: Union[Dict[str, Any], AttributeEntry, List[AttributeEntry]]):
        """
        Send attributes on behalf of a connected device.

        :param device_name: Name of the device
        :param attributes: Attributes to send
        """
        if device_name not in self._connected_devices:
            logger.warning("Cannot send attributes for disconnected device %s", device_name)
            return

        # Convert attributes to the appropriate format
        payload = self._prepare_attributes_payload(device_name, attributes)

        # Publish attributes
        await self._mqtt_manager.publish(
            mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC,
            dumps(payload),
            qos=1
        )

        logger.debug("Sent attributes for device %s: %s", device_name, payload)

    async def gw_send_rpc_reply(self, device_name: str, request_id: int, response: Dict[str, Any]):
        """
        Send an RPC response on behalf of a connected device.

        :param device_name: Name of the device
        :param request_id: ID of the RPC request
        :param response: Response data
        """
        if device_name not in self._connected_devices:
            logger.warning("Cannot send RPC reply for disconnected device %s", device_name)
            return

        # Prepare RPC response payload
        payload = {
            "device": device_name,
            "id": request_id,
            "data": response
        }

        # Publish RPC response
        await self._mqtt_manager.publish(
            mqtt_topics.GATEWAY_RPC_RESPONSE_TOPIC,
            dumps(payload),
            qos=1
        )

        logger.debug("Sent RPC response for device %s, request %s: %s", device_name, request_id, response)

    async def gw_request_shared_attributes(self, device_name: str, keys: List[str], callback: Callable[[Dict[str, Any]], Awaitable[None]]):
        """
        Request shared attributes for a connected device.

        :param device_name: Name of the device
        :param keys: List of attribute keys to request
        :param callback: Callback function to handle the response
        """
        if device_name not in self._connected_devices:
            logger.warning("Cannot request attributes for disconnected device %s", device_name)
            return

        # TODO: Implement attribute request handling with callbacks

        # Prepare attribute request payload
        request_id = 1  # TODO: Generate unique request ID
        payload = {
            "device": device_name,
            "keys": keys,
            "id": request_id
        }

        # Publish attribute request
        await self._mqtt_manager.publish(
            mqtt_topics.GATEWAY_ATTRIBUTES_REQUEST_TOPIC,
            dumps(payload),
            qos=1
        )

        logger.debug("Requested shared attributes for device %s: %s", device_name, keys)

    def set_device_attribute_update_callback(self, callback: Callable[[AttributeUpdate], Awaitable[None]]):
        """
        Set callback for device attribute updates.

        :param callback: Callback function
        """
        self._device_attribute_update_callback = callback

    def set_device_rpc_request_callback(self, callback: Callable[[str, str, Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        """
        Set callback for device RPC requests.

        :param callback: Callback function that takes device name, method, and params
        """
        self._device_rpc_request_callback = callback

    def set_device_disconnect_callback(self, callback: Callable[[str], Awaitable[None]]):
        """
        Set callback for device disconnections.

        :param callback: Callback function
        """
        self._device_disconnect_callback = callback

    def _prepare_telemetry_payload(self, device_name: str, telemetry: Union[Dict[str, Any], TimeseriesEntry, List[TimeseriesEntry]]) -> Dict[str, Any]:
        """
        Prepare telemetry payload for gateway API.

        :param device_name: Name of the device
        :param telemetry: Telemetry data
        :return: Formatted payload
        """
        if isinstance(telemetry, dict):
            # Simple key-value telemetry
            return {device_name: telemetry}

        elif isinstance(telemetry, TimeseriesEntry):
            # Single TimeseriesEntry
            if telemetry.ts:
                return {device_name: {"ts": telemetry.ts, "values": {telemetry.key: telemetry.value}}}
            else:
                return {device_name: {telemetry.key: telemetry.value}}

        elif isinstance(telemetry, list):
            # List of TimeseriesEntry objects
            # Group by timestamp
            ts_groups = {}
            for entry in telemetry:
                ts = entry.ts or 0
                if ts not in ts_groups:
                    ts_groups[ts] = {}
                ts_groups[ts][entry.key] = entry.value

            if len(ts_groups) == 1 and 0 in ts_groups:
                # No timestamps, just values
                return {device_name: ts_groups[0]}
            else:
                # With timestamps
                result = []
                for ts, values in ts_groups.items():
                    if ts > 0:
                        result.append({"ts": ts, "values": values})
                    else:
                        result.append({"values": values})
                return {device_name: result}

        # Fallback
        logger.warning("Unsupported telemetry format: %s", type(telemetry))
        return {device_name: {}}

    def _prepare_attributes_payload(self, device_name: str, attributes: Union[Dict[str, Any], AttributeEntry, List[AttributeEntry]]) -> Dict[str, Any]:
        """
        Prepare attributes payload for gateway API.

        :param device_name: Name of the device
        :param attributes: Attributes data
        :return: Formatted payload
        """
        if isinstance(attributes, dict):
            # Simple key-value attributes
            return {device_name: attributes}

        elif isinstance(attributes, AttributeEntry):
            # Single AttributeEntry
            return {device_name: {attributes.key: attributes.value}}

        elif isinstance(attributes, list):
            # List of AttributeEntry objects
            attrs = {}
            for entry in attributes:
                attrs[entry.key] = entry.value
            return {device_name: attrs}

        # Fallback
        logger.warning("Unsupported attributes format: %s", type(attributes))
        return {device_name: {}}
