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
from abc import abstractmethod, ABC
from collections import defaultdict
from datetime import datetime, UTC
from itertools import chain
from typing import List, Optional, Tuple, Dict, Any, Union

from orjson import loads, dumps

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.constants.mqtt_topics import GATEWAY_TELEMETRY_TOPIC, GATEWAY_ATTRIBUTES_TOPIC, \
    GATEWAY_CONNECT_TOPIC, GATEWAY_DISCONNECT_TOPIC
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.device_disconnect_message import DeviceDisconnectMessage
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_attribute_update import GatewayAttributeUpdate
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse

logger = get_logger(__name__)


class GatewayMessageAdapter(ABC):
    """
    Adapter for converting events to uplink messages and received messages to events.
    """

    @abstractmethod
    def build_uplink_payloads(
        self,
        messages: List[DeviceUplinkMessage]
    ) -> List[Tuple[str, bytes, int, List[Optional[asyncio.Future[PublishResult]]]]]:
        """
        Build a list of topic-payload pairs from the given messages.
        Each pair consists of a topic string, payload bytes, the number of datapoints,
        and a list of futures for delivery confirmation.
        """
        pass

    @abstractmethod
    def build_device_connect_message_payload(self, device_connect_message: DeviceConnectMessage) -> Tuple[str, bytes]:
        """
        Build the payload for a device connect message.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def build_device_disconnect_message_payload(self, device_disconnect_message: DeviceDisconnectMessage) -> Tuple[str, bytes]:
        """
        Build the payload for a device disconnect message.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def build_gateway_attribute_request_payload(self, attribute_request: GatewayAttributeRequest) -> Tuple[str, bytes]:
        """
        Build the payload for a gateway attribute request.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def parse_attribute_update(self, payload: bytes) -> GatewayAttributeUpdate:
        """
        Parse the attribute update payload into an GatewayAttributeUpdate.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def parse_gateway_attribute_response(self, gateway_attribute_request: GatewayAttributeRequest, payload: bytes) -> Union[GatewayRequestedAttributeResponse, None]:
        """
        Parse the gateway attribute response payload into an GatewayAttributeResponse.
        This method should be implemented to handle the specific format of the payload.
        """
        pass


class JsonGatewayMessageAdapter(GatewayMessageAdapter):
    """
    JSON implementation of GatewayMessageAdapter.
    Builds uplink payloads from uplink message objects and parses JSON payloads into GatewayEvent objects.
    """

    def build_uplink_payloads(self, messages: List[DeviceUplinkMessage]) -> List[Tuple[str, bytes, int, List[Optional[asyncio.Future[PublishResult]]]]]:
        """
        Build a list of topic-payload pairs from the given messages.
        Each pair consists of a topic string, payload bytes, the number of datapoints,
        and a list of futures for delivery confirmation.
        """
        try:
            if not messages:
                logger.trace("No messages to process in build_topic_payloads.")
                return []

            result: List[Tuple[str, bytes, int, List[Optional[asyncio.Future[PublishResult]]]]] = []
            device_groups: Dict[str, List[DeviceUplinkMessage]] = defaultdict(list)

            for msg in messages:
                device_name = msg.device_name
                device_groups[device_name].append(msg)
                logger.trace("Queued message for device='%s'", device_name)

            logger.trace("Processing %d device group(s).", len(device_groups))

            gateway_timeseries_message = {}
            gateway_attributes_message = {}
            gateway_timeseries_device_datapoints_counts: Dict[str, int] = {}
            gateway_attributes_device_datapoints_counts: Dict[str, int] = {}
            gateway_timeseries_delivery_futures: Dict[str, List[Optional[asyncio.Future[PublishResult]]]] = {}
            gateway_attributes_delivery_futures: Dict[str, List[Optional[asyncio.Future[PublishResult]]]] = {}
            for device, device_msgs in device_groups.items():
                if device not in gateway_timeseries_message:
                    gateway_timeseries_message[device] = []
                    gateway_timeseries_delivery_futures[device] = []
                if device not in gateway_attributes_message:
                    gateway_attributes_message[device] = []
                    gateway_attributes_delivery_futures[device] = []
                telemetry_msgs: List[DeviceUplinkMessage] = [m for m in device_msgs if m.has_timeseries()]
                attr_msgs: List[DeviceUplinkMessage] = [m for m in device_msgs if m.has_attributes()]
                logger.trace("Device '%s' - telemetry: %d, attributes: %d",
                             device, len(telemetry_msgs), len(attr_msgs))

                # TODO: Recommended to add message splitter to handle large messages and split them into smaller batches
                for ts_batch in telemetry_msgs:
                    packed_ts = JsonGatewayMessageAdapter.pack_timeseries(ts_batch)
                    gateway_timeseries_message[device].append(packed_ts)
                    count = ts_batch.timeseries_datapoint_count()
                    gateway_timeseries_device_datapoints_counts[device] = gateway_timeseries_device_datapoints_counts.get(device, 0) + count
                    gateway_timeseries_delivery_futures[device] = ts_batch.get_delivery_futures()
                    logger.trace("Built telemetry payload for device='%s' with %d datapoints", device, count)

                for attr_batch in attr_msgs:
                    packed_attrs = JsonGatewayMessageAdapter.pack_attributes(attr_batch)
                    count = attr_batch.attributes_datapoint_count()
                    gateway_attributes_message[device].append(packed_attrs)
                    gateway_attributes_device_datapoints_counts[device] = gateway_attributes_device_datapoints_counts.get(device, 0) + count
                    logger.trace("Built attribute payload for device='%s' with %d attributes", device, count)
                if telemetry_msgs:
                    result.append((GATEWAY_TELEMETRY_TOPIC,
                                  dumps(gateway_timeseries_message[device]),
                                  gateway_timeseries_device_datapoints_counts[device],
                                  gateway_timeseries_delivery_futures[device]))
                if attr_msgs:
                    result.append((GATEWAY_ATTRIBUTES_TOPIC,
                                  dumps(gateway_attributes_message[device]),
                                  gateway_attributes_device_datapoints_counts[device],
                                  gateway_attributes_delivery_futures[device]))

            logger.trace("Generated %d topic-payload entries.", len(result))

            return result
        except Exception as e:
            logger.error("Error building topic-payloads: %s", str(e))
            logger.debug("Exception details: %s", e, exc_info=True)
            raise

    def build_device_connect_message_payload(self, device_connect_message: DeviceConnectMessage) -> Tuple[str, bytes]:
        """
        Build the payload for a device connect message.
        This method serializes the DeviceConnectMessage to JSON format.
        """
        try:
            payload = dumps(device_connect_message.to_payload_format())
            logger.trace("Built device connect message payload for device='%s'", device_connect_message.device_name)
            return GATEWAY_CONNECT_TOPIC, payload
        except Exception as e:
            logger.error("Failed to build device connect message payload: %s", str(e))
            raise ValueError("Invalid device connect message format") from e

    def build_device_disconnect_message_payload(self, device_disconnect_message: DeviceDisconnectMessage) -> Tuple[str, bytes]:
        """
        Build the payload for a device disconnect message.
        This method serializes the device name to JSON format.
        """
        try:
            payload = dumps(device_disconnect_message.to_payload_format())
            logger.trace("Built device disconnect message payload for device='%s'", device_disconnect_message.device_name)
            return GATEWAY_DISCONNECT_TOPIC, payload
        except Exception as e:
            logger.error("Failed to build device disconnect message payload: %s", str(e))
            raise ValueError("Invalid device disconnect message format") from e

    def build_gateway_attribute_request_payload(self, attribute_request: GatewayAttributeRequest) -> Tuple[str, bytes]:
        """
        Build the payload for a gateway attribute request.
        This method serializes the GatewayAttributeRequest to JSON format.
        """
        try:
            payload = dumps(attribute_request.to_payload_format())
            logger.trace("Built gateway attribute request payload for device='%s'", attribute_request.device_name)
            return GATEWAY_ATTRIBUTES_TOPIC, payload
        except Exception as e:
            logger.error("Failed to build gateway attribute request payload: %s", str(e))
            raise ValueError("Invalid gateway attribute request format") from e

    def parse_attribute_update(self, payload: bytes) -> GatewayAttributeUpdate:
        try:
            data = loads(payload.decode('utf-8'))
            device_name = data['device_name']
            attribute_update = AttributeUpdate._deserialize_from_dict(data['data'])  # noqa
            return GatewayAttributeUpdate(device_name=device_name, attribute_update=attribute_update)
        except Exception as e:
            logger.error("Failed to parse attribute update: %s", str(e))
            raise ValueError("Invalid attribute update format") from e

    def parse_gateway_attribute_response(self, gateway_attribute_request: GatewayAttributeRequest, payload: bytes) -> Union[GatewayRequestedAttributeResponse, None]:
        try:
            data = loads(payload.decode('utf-8'))
            device_name = data['device_name']
            client = []
            shared = []
            if 'value' in data and not ((len(gateway_attribute_request.client_keys) == 1 and len(gateway_attribute_request.shared_keys) == 0)
                                        or (len(gateway_attribute_request.client_keys) == 0 and len(gateway_attribute_request.shared_keys) == 1)):
                # TODO: Skipping case when requested several attributes, but only one is returned, issue on the platform
                logger.warning("Received gateway attribute response with single key, but multiply keys expected. "
                               "Request keys: %s, Response keys: %s", list(*gateway_attribute_request.client_keys, *gateway_attribute_request.shared_keys), data['value'])
                return None
            elif 'value' in data:
                if len(gateway_attribute_request.client_keys) == 1:
                    client= [AttributeEntry(gateway_attribute_request.client_keys[0], data['value'])]
                elif len(gateway_attribute_request.shared_keys) == 1:
                    shared = [AttributeEntry(gateway_attribute_request.shared_keys[0], data['value'])]
            elif 'data' in data:
                if len(gateway_attribute_request.client_keys) > 0:
                    client = [AttributeEntry(k, v) for k, v in data['data'].get('client', {}).items() if k in gateway_attribute_request.client_keys]
                if len(gateway_attribute_request.shared_keys) > 0:
                    shared = [AttributeEntry(k, v) for k, v in data['data'].get('shared', {}).items() if k in gateway_attribute_request.shared_keys]
            return GatewayRequestedAttributeResponse(device_name=device_name, request_id=gateway_attribute_request.request_id, shared=shared, client=client)
        except Exception as e:
            logger.error("Failed to parse gateway attribute response: %s", str(e))
            raise ValueError("Invalid gateway attribute response format") from e

    @staticmethod
    def pack_attributes(msg: DeviceUplinkMessage) -> Dict[str, Any]:
        logger.trace("Packing %d attribute(s)", len(msg.attributes))
        return {attr.key: attr.value for attr in msg.attributes}

    @staticmethod
    def pack_timeseries(msg: DeviceUplinkMessage) -> List[Dict[str, Any]]:
        now_ts = int(datetime.now(UTC).timestamp() * 1000)
        packed = [
            {"ts": entry.ts or now_ts, "values": {entry.key: entry.value}}
            for entry in chain.from_iterable(msg.timeseries.values())
        ]
        logger.trace("Packed %d timeseries entry(s)", len(packed))

        return packed
