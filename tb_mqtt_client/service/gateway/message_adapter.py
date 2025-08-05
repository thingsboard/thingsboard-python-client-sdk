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

from abc import abstractmethod, ABC
from collections import defaultdict
from datetime import datetime, UTC
from typing import List, Dict, Any, Union, Optional

from orjson import loads, dumps

from tb_mqtt_client.common.async_utils import future_map
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.constants.mqtt_topics import GATEWAY_TELEMETRY_TOPIC, GATEWAY_ATTRIBUTES_TOPIC, \
    GATEWAY_CONNECT_TOPIC, GATEWAY_DISCONNECT_TOPIC, GATEWAY_ATTRIBUTES_REQUEST_TOPIC, GATEWAY_RPC_TOPIC, \
    GATEWAY_CLAIM_TOPIC
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.device_disconnect_message import DeviceDisconnectMessage
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_attribute_update import GatewayAttributeUpdate
from tb_mqtt_client.entities.gateway.gateway_claim_request import GatewayClaimRequest
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessage
from tb_mqtt_client.service.gateway.message_splitter import GatewayMessageSplitter

logger = get_logger(__name__)


class GatewayMessageAdapter(ABC):
    """
    Adapter for converting events to uplink messages and received messages to events.
    """

    def __init__(self, max_payload_size: Optional[int] = None, max_datapoints: Optional[int] = None):
        self._splitter = GatewayMessageSplitter(max_payload_size, max_datapoints)
        logger.trace("GatewayMessageAdapter initialized with max_payload_size=%s, max_datapoints=%s",
                     max_payload_size, max_datapoints)

    @property
    def splitter(self) -> GatewayMessageSplitter:
        """
        Returns the message splitter instance used by this adapter.
        This allows for splitting messages into smaller parts if needed.
        """
        return self._splitter

    @abstractmethod
    def build_uplink_messages(
            self,
            messages: List[MqttPublishMessage]
    ) -> List[MqttPublishMessage]:
        """
        Build a list of topic-payload pairs from the given messages.
        Each pair consists of a topic string, payload bytes, the number of datapoints,
        and a list of futures for delivery confirmation.
        """
        pass

    @abstractmethod
    def build_device_connect_message_payload(self,
                                             device_connect_message: DeviceConnectMessage,
                                             qos) -> MqttPublishMessage:
        """
        Build the payload for a device connect message.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def build_device_disconnect_message_payload(self,
                                                device_disconnect_message: DeviceDisconnectMessage,
                                                qos) -> MqttPublishMessage:
        """
        Build the payload for a device disconnect message.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def build_gateway_attribute_request_payload(self,
                                                attribute_request: GatewayAttributeRequest,
                                                qos) -> MqttPublishMessage:
        """
        Build the payload for a gateway attribute request.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def build_rpc_response_payload(self, rpc_response: GatewayRPCResponse, qos) -> MqttPublishMessage:
        """
        Build the payload for a gateway RPC response.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def build_claim_request_payload(self, claim_request: GatewayClaimRequest, qos) -> MqttPublishMessage:
        """
        Build the payload for a gateway claim request.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def parse_attribute_update(self, data: Dict[str, Any]) -> GatewayAttributeUpdate:
        """
        Parse the attribute update payload into an GatewayAttributeUpdate.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def parse_gateway_requested_attribute_response(self,
                                                   gateway_attribute_request: GatewayAttributeRequest,
                                                   data: Dict[str, Any]) -> Union[
        GatewayRequestedAttributeResponse, None]:
        """
        Parse the gateway attribute response data into an GatewayAttributeResponse.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def parse_rpc_request(self, topic: str, data: Dict[str, Any]) -> GatewayRPCRequest:
        """
        Parse the RPC request from the given topic and payload.
        This method should be implemented to handle the specific format of the RPC request.
        """
        pass

    @abstractmethod
    def deserialize_to_dict(self, payload: bytes) -> Dict[str, Any]:
        """
        Deserialize incoming payload into a dictionary format, required for parsing responses.
        This method should be implemented to handle the specific format of the response.
        """
        pass


class JsonGatewayMessageAdapter(GatewayMessageAdapter):
    """
    JSON implementation of GatewayMessageAdapter.
    Builds uplink payloads from uplink message objects and parses JSON payloads into GatewayEvent objects.
    """

    def build_uplink_messages(self, messages: List[MqttPublishMessage]) -> List[MqttPublishMessage]:
        if not messages:
            logger.trace("No messages to process in build_uplink_messages.")
            return []

        result: List[MqttPublishMessage] = []
        device_groups: Dict[str, List[GatewayUplinkMessage]] = defaultdict(list)
        qos = messages[0].qos

        # Group by device
        for mqtt_msg in messages:
            payload = mqtt_msg.payload
            if isinstance(payload, GatewayUplinkMessage):
                device_groups[payload.device_name].append(payload)
                logger.trace("Queued GatewayUplinkMessage for device='%s'", payload.device_name)
            else:
                result.append(mqtt_msg)
                logger.debug("Non-GatewayUplinkMessage found, sending as is: %s", type(payload).__name__)

        # Process each device group
        for device_name, group_msgs in device_groups.items():
            telemetry_msgs = [m for m in group_msgs if m.has_timeseries()]
            attr_msgs = [m for m in group_msgs if m.has_attributes()]
            built_child_messages: List[MqttPublishMessage] = []

            if telemetry_msgs:
                for ts_batch in self._splitter.split_timeseries(telemetry_msgs):
                    payload_dict = {device_name: self.pack_timeseries(ts_batch)}
                    payload_bytes = dumps(payload_dict)
                    count = ts_batch.timeseries_datapoint_count()
                    futures = ts_batch.get_delivery_futures() or []

                    mqtt_msg = MqttPublishMessage(
                        topic=GATEWAY_TELEMETRY_TOPIC,
                        payload=payload_bytes,
                        qos=qos,
                        datapoints=count,
                        delivery_futures=futures,
                        main_ts=ts_batch.main_ts,
                        original_payload=ts_batch
                    )
                    result.append(mqtt_msg)
                    built_child_messages.append(mqtt_msg)

            if attr_msgs:
                for attr_batch in self._splitter.split_attributes(attr_msgs):
                    payload_dict = {device_name: self.pack_attributes(attr_batch)}
                    payload_bytes = dumps(payload_dict)
                    count = attr_batch.attributes_datapoint_count()
                    futures = attr_batch.get_delivery_futures() or []

                    mqtt_msg = MqttPublishMessage(
                        topic=GATEWAY_ATTRIBUTES_TOPIC,
                        payload=payload_bytes,
                        qos=qos,
                        datapoints=count,
                        delivery_futures=futures,
                        main_ts=attr_batch.main_ts,
                        original_payload=attr_batch
                    )
                    result.append(mqtt_msg)
                    built_child_messages.append(mqtt_msg)

            # Link parent futures to child delivery futures
            parent_futures = [f for m in messages for f in (m.delivery_futures or [])
                              if isinstance(m.payload, GatewayUplinkMessage) and m.payload.device_name == device_name]
            for parent in parent_futures:
                for child_msg in built_child_messages:
                    for child in child_msg.delivery_futures or []:
                        future_map.register(parent, [child])

        logger.trace("Generated %d MqttPublishMessage(s) for gateway uplink.", len(result))
        return result

    def build_device_connect_message_payload(self,
                                             device_connect_message: DeviceConnectMessage,
                                             qos) -> MqttPublishMessage:
        """
        Build the payload for a device connect message.
        This method serializes the DeviceConnectMessage to JSON format.
        """
        try:
            payload = dumps(device_connect_message.to_payload_format())
            logger.trace("Built device connect message payload for device='%s'",
                         device_connect_message.device_name)
            return MqttPublishMessage(GATEWAY_CONNECT_TOPIC, payload, qos=1)
        except Exception as e:
            logger.error("Failed to build device connect message payload: %s", str(e))
            raise ValueError("Invalid device connect message format") from e

    def build_device_disconnect_message_payload(self,
                                                device_disconnect_message: DeviceDisconnectMessage,
                                                qos) -> MqttPublishMessage:
        """
        Build the payload for a device disconnect message.
        This method serializes the DeviceDisconnectMessage to JSON format.
        """
        try:
            payload = dumps(device_disconnect_message.to_payload_format())
            logger.trace("Built device disconnect message payload for device='%s'",
                         device_disconnect_message.device_name)
            return MqttPublishMessage(GATEWAY_DISCONNECT_TOPIC, payload, qos)
        except Exception as e:
            logger.error("Failed to build device disconnect message payload: %s", str(e))
            raise ValueError("Invalid device disconnect message format") from e

    def build_gateway_attribute_request_payload(self,
                                                attribute_request: GatewayAttributeRequest,
                                                qos) -> MqttPublishMessage:
        """
        Build the payload for a gateway attribute request.
        This method serializes the GatewayAttributeRequest to JSON format.
        """
        try:
            payload = dumps(attribute_request.to_payload_format())
            logger.trace("Built gateway attribute request payload for device='%s'",
                         attribute_request.device_session.device_info.device_name)
            return MqttPublishMessage(GATEWAY_ATTRIBUTES_REQUEST_TOPIC, payload, qos)
        except Exception as e:
            logger.error("Failed to build gateway attribute request payload: %s", str(e))
            raise ValueError("Invalid gateway attribute request format") from e

    def build_rpc_response_payload(self, rpc_response: GatewayRPCResponse, qos) -> MqttPublishMessage:
        """
        Build the payload for a gateway RPC response.
        This method serializes the GatewayRPCResponse to JSON format.
        """
        try:
            payload = dumps(rpc_response.to_payload_format())
            logger.trace("Built RPC response payload for device='%s', request_id=%i",
                         rpc_response.device_name, rpc_response.request_id)
            return MqttPublishMessage(GATEWAY_RPC_TOPIC, payload, qos)
        except Exception as e:
            logger.error("Failed to build RPC response payload: %s", str(e))
            raise ValueError("Invalid RPC response format") from e

    def build_claim_request_payload(self, claim_request: GatewayClaimRequest, qos) -> MqttPublishMessage:
        """
        Build the payload for a gateway claim request.
        This method serializes the GatewayClaimRequest to JSON format.
        """
        try:
            payload = dumps(claim_request.to_payload_format())
            logger.trace("Built claim request payload for devices: %s",
                         list(claim_request.devices_requests.keys()))
            return MqttPublishMessage(GATEWAY_CLAIM_TOPIC, payload, qos)
        except Exception as e:
            logger.error("Failed to build claim request payload: %s", str(e))
            raise ValueError("Invalid claim request format") from e

    def parse_attribute_update(self, data: Dict[str, Any]) -> GatewayAttributeUpdate:
        try:
            device_name = data['device']
            attribute_update = AttributeUpdate._deserialize_from_dict(data['data'])  # noqa
            return GatewayAttributeUpdate(device_name=device_name, attribute_update=attribute_update)
        except Exception as e:
            logger.error("Failed to parse attribute update: %s", str(e))
            raise ValueError("Invalid attribute update format") from e

    def parse_gateway_requested_attribute_response(self,
                                                   gateway_attribute_request: GatewayAttributeRequest,
                                                   data: Dict[str, Any]) -> Union[
        GatewayRequestedAttributeResponse, None]:
        """
        Parse the gateway attribute response data into a GatewayRequestedAttributeResponse.
        This method extracts the device name, shared and client attributes from the payload.
        """
        try:
            device_name = data['device']
            client = []
            shared = []
            client_keys_empty = gateway_attribute_request.client_keys is None
            shared_keys_empty = gateway_attribute_request.shared_keys is None
            if ('value' in data
                    and not (((not client_keys_empty and len(gateway_attribute_request.client_keys) == 1)
                              and not gateway_attribute_request.shared_keys)
                             or ((not shared_keys_empty and len(gateway_attribute_request.shared_keys) == 1)
                                 and not gateway_attribute_request.client_keys))):
                # TODO: Skipping case when requested several attributes, but only one is returned, issue on the platform
                logger.warning("Received gateway attribute response with single key, but multiply keys expected. "
                               "Request keys: %s, Response value: %r",
                               gateway_attribute_request.client_keys + gateway_attribute_request.shared_keys,
                               data['value'])
                client = []
                shared = []
            elif 'value' in data:
                if not client_keys_empty and len(gateway_attribute_request.client_keys) == 1:
                    client = [AttributeEntry(gateway_attribute_request.client_keys[0], data['value'])]
                elif not shared_keys_empty and len(gateway_attribute_request.shared_keys) == 1:
                    shared = [AttributeEntry(gateway_attribute_request.shared_keys[0], data['value'])]
            elif 'values' in data:
                if not client_keys_empty and len(gateway_attribute_request.client_keys) > 0:
                    client = [AttributeEntry(k, v) for k, v in data['values'].items() if
                              k in gateway_attribute_request.client_keys]
                if not shared_keys_empty and len(gateway_attribute_request.shared_keys) > 0:
                    shared = [AttributeEntry(k, v) for k, v in data['values'].items() if
                              k in gateway_attribute_request.shared_keys]
            return GatewayRequestedAttributeResponse(device_name=device_name,
                                                     request_id=gateway_attribute_request.request_id,
                                                     shared=shared,
                                                     client=client)
        except Exception as e:
            logger.error("Failed to parse gateway requested attribute response: %s", str(e))
            raise ValueError("Invalid gateway requested attribute response format") from e

    def parse_rpc_request(self, topic: str, data: Dict[str, Any]) -> GatewayRPCRequest:
        """
        Parse the RPC request from the given topic and payload.
        This method deserializes the payload into a GatewayRPCRequest object.
        """
        try:
            return GatewayRPCRequest._deserialize_from_dict(data)  # noqa
        except Exception as e:
            logger.error("Failed to parse RPC request: %s", str(e))
            raise ValueError("Invalid RPC request format") from e

    def deserialize_to_dict(self, payload: bytes) -> Dict[str, Any]:
        """
        Deserialize incoming payload into a dictionary format, required for parsing responses.
        This method decodes the payload from bytes to a string and then loads it as a JSON object.
        """
        try:
            data = loads(payload.decode('utf-8'))
            return data
        except Exception as e:
            logger.error("Failed to deserialize requested attribute response: %s", str(e))
            raise ValueError("Invalid requested attribute response format") from e

    @staticmethod
    def pack_attributes(msg: GatewayUplinkMessage) -> Dict[str, Any]:
        logger.trace("Packing %d attribute(s)", len(msg.attributes))
        return {attr.key: attr.value for attr in msg.attributes}

    @staticmethod
    def pack_timeseries(msg: 'GatewayUplinkMessage') -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        entries = [e for entries in msg.timeseries.values() for e in entries]
        if not entries:
            return {}

        all_ts_none = True
        for e in entries:
            if e.ts is not None:
                all_ts_none = False
                break

        if all_ts_none:
            result = {e.key: e.value for e in entries}
            return [{"ts": msg.main_ts, "values": result}] if msg.main_ts is not None else [result]

        now_ts = msg.main_ts if msg.main_ts is not None else int(datetime.now(UTC).timestamp() * 1000)
        grouped = defaultdict(dict)
        for e in entries:
            ts = e.ts if e.ts is not None else now_ts
            grouped[ts][e.key] = e.value

        return [{"ts": ts, "values": values} for ts, values in grouped.items()]
