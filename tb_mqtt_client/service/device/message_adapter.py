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
from abc import ABC, abstractmethod
from itertools import chain
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Dict, List, Tuple, Optional, Union

from orjson import dumps, loads

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.constants.mqtt_topics import DEVICE_TELEMETRY_TOPIC, DEVICE_ATTRIBUTES_TOPIC
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, ProvisioningCredentialsType
from tb_mqtt_client.entities.data.provisioning_response import ProvisioningResponse
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.service.message_splitter import MessageSplitter

logger = get_logger(__name__)


class MessageAdapter(ABC):
    def __init__(self, max_payload_size: Optional[int] = None, max_datapoints: Optional[int] = None):
        self._splitter = MessageSplitter(max_payload_size, max_datapoints)
        logger.trace("MessageDispatcher initialized with max_payload_size=%s, max_datapoints=%s",
                     max_payload_size, max_datapoints)

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
    def build_attribute_request(self, request: AttributeRequest) -> Tuple[str, bytes]:
        """
        Build the payload for an attribute request response.
        This method should return a tuple of topic and payload bytes.
        """
        pass

    @abstractmethod
    def build_claim_request(self, claim_request) -> Tuple[str, bytes]:
        """
        Build the payload for a claim request.
        This method should return a tuple of topic and payload bytes.
        """

    @abstractmethod
    def build_rpc_request(self, rpc_request: RPCRequest) -> Tuple[str, bytes]:
        """
        Build the payload for an RPC request.
        This method should return a tuple of topic and payload bytes.
        """
        pass

    @abstractmethod
    def build_rpc_response(self, rpc_response: RPCResponse) -> Tuple[str, bytes]:
        """
        Build the payload for an RPC response.
        This method should return a tuple of topic and payload bytes.
        """
        pass

    @abstractmethod
    def build_provision_request(self, provision_request) -> Tuple[str, bytes]:
        """
        Build the payload for a device provisioning request.
        This method should return a tuple of topic and payload bytes.
        """
        pass

    @abstractmethod
    def splitter(self) -> MessageSplitter:
        """
        Get the message splitter instance.
        """
        pass

    @abstractmethod
    def parse_requested_attribute_response(self, topic: str, payload: bytes) -> RequestedAttributeResponse:
        """
        Parse the attribute request response payload into an AttributeRequestResponse.
        This method should be implemented to handle the specific format of the topic and payload.
        """
        pass

    @abstractmethod
    def parse_attribute_update(self, payload: bytes) -> AttributeUpdate:
        """
        Parse the attribute update payload into an AttributeUpdate.
        This method should be implemented to handle the specific format of the payload.
        """
        pass

    @abstractmethod
    def parse_rpc_request(self, topic: str, payload: bytes) -> RPCRequest:
        """
        Parse the RPC request from the given topic and payload.
        This method should be implemented to handle the specific format of the RPC request.
        """
        pass

    @abstractmethod
    def parse_rpc_response(self, topic: str, payload: Union[bytes, Exception]) -> RPCResponse:
        """
        Parse the RPC response from the given topic and payload.
        This method should be implemented to handle the specific format of the RPC response.
        """
        pass

    @abstractmethod
    def parse_provisioning_response(self, provisioning_request: ProvisioningRequest, payload: bytes) -> 'ProvisioningResponse':
        """
        Parse the provisioning response from the given payload.
        This method should be implemented to handle the specific format of the provisioning response.
        """
        pass


class JsonMessageAdapter(MessageAdapter):
    """
    A concrete implementation of MessageDispatcher that operates with JSON payloads.
    """
    def __init__(self, max_payload_size: Optional[int] = None, max_datapoints: Optional[int] = None):
        super().__init__(max_payload_size, max_datapoints)
        logger.trace("JsonMessageDispatcher created.")

    def parse_requested_attribute_response(self, topic: str, payload: bytes) -> RequestedAttributeResponse:
        """
        Parse the attribute request response payload into a RequestedAttributeResponse.
        :param topic: The MQTT topic of the requested attribute response.
        :param payload: The raw bytes of the payload.
        :return: An instance of RequestedAttributeResponse.
        """
        try:
            request_id = int(topic.split("/")[-1])
            data = loads(payload)
            logger.trace("Parsing attribute request response from payload: %s", data)
            if not isinstance(data, dict):
                logger.error("Invalid requested attribute response format: expected dict, got %s", type(data).__name__)
                raise ValueError("Invalid requested attribute response format")
            data["request_id"] = request_id  # Add request_id to the data dictionary
            return RequestedAttributeResponse.from_dict(data)
        except Exception as e:
            logger.error("Failed to parse attribute request response: %s", str(e))
            raise ValueError("Invalid attribute request response format") from e

    def parse_attribute_update(self, payload: bytes) -> AttributeUpdate:
        """
        Parse the attribute update payload into an AttributeUpdate.
        :param payload: The raw bytes of the payload.
        :return: An instance of AttributeUpdate.
        """
        try:
            data = loads(payload)
            logger.trace("Parsing attribute update from payload: %s", data)
            return AttributeUpdate._deserialize_from_dict(data)
        except Exception as e:
            logger.error("Failed to parse attribute update: %s", str(e))
            raise ValueError("Invalid attribute update format") from e

    def parse_rpc_request(self, topic: str, payload: bytes) -> RPCRequest:
        """
        Parse the RPC request from the given topic and payload.
        :param topic: The MQTT topic of the RPC request.
        :param payload: The raw bytes of the payload.
        :return: An instance of RPCRequest.
        """
        try:
            request_id = int(topic.split("/")[-1])
            parsed = loads(payload)
            data = RPCRequest._deserialize_from_dict(request_id, parsed)  # noqa
            return data
        except Exception as e:
            logger.error("Failed to parse RPC request: %s", str(e))
            raise ValueError("Invalid RPC request format") from e

    def parse_rpc_response(self, topic: str, payload: Union[bytes, Exception]) -> RPCResponse:
        """
        Parse the RPC response from the given topic and payload.
        :param topic: The MQTT topic of the RPC response.
        :param payload: The raw bytes of the payload.
        :return: An instance of RPCResponse.
        """
        try:
            request_id = int(topic.split("/")[-1])
            if isinstance(payload, Exception):
                data = RPCResponse.build(request_id, error=payload)
            else:
                parsed = loads(payload)
                data = RPCResponse.build(request_id, parsed)  # noqa
            return data
        except Exception as e:
            logger.error("Failed to parse RPC response: %s", str(e))
            raise ValueError("Invalid RPC response format") from e

    def parse_provisioning_response(self, provisioning_request: ProvisioningRequest, payload: bytes) -> 'ProvisioningResponse':
        """
        Parse the provisioning response from the given payload.
        :param provisioning_request: The ProvisioningRequest that initiated the provisioning.
        :param payload: The raw bytes of the payload.
        :return: An instance of ProvisioningResponse.
        """
        try:
            data = loads(payload)
            logger.trace("Parsing provisioning response from payload: %s", data)
            return ProvisioningResponse.build(provisioning_request, data)
        except Exception as e:
            logger.error("Failed to parse provisioning response: %s", str(e))
            return ProvisioningResponse.build(provisioning_request, {"status": "FAILURE", "errorMsg": str(e)})

    @property
    def splitter(self) -> MessageSplitter:
        return self._splitter

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

            for device, device_msgs in device_groups.items():
                telemetry_msgs = [m for m in device_msgs if m.has_timeseries()]
                attr_msgs = [m for m in device_msgs if m.has_attributes()]
                logger.trace("Device '%s' - telemetry: %d, attributes: %d",
                             device, len(telemetry_msgs), len(attr_msgs))

                for ts_batch in self._splitter.split_timeseries(telemetry_msgs):
                    payload = JsonMessageAdapter.build_payload(ts_batch, True)
                    count = ts_batch.timeseries_datapoint_count()
                    result.append((DEVICE_TELEMETRY_TOPIC, payload, count, ts_batch.get_delivery_futures()))
                    logger.trace("Built telemetry payload for device='%s' with %d datapoints", device, count)

                for attr_batch in self._splitter.split_attributes(attr_msgs):
                    payload = JsonMessageAdapter.build_payload(attr_batch, False)
                    count = len(attr_batch.attributes)
                    result.append((DEVICE_ATTRIBUTES_TOPIC, payload, count, attr_batch.get_delivery_futures()))
                    logger.trace("Built attribute payload for device='%s' with %d attributes", device, count)

            logger.trace("Generated %d topic-payload entries.", len(result))

            return result
        except Exception as e:
            logger.error("Error building topic-payloads: %s", str(e))
            logger.debug("Exception details: %s", e, exc_info=True)
            raise

    def build_attribute_request(self, request: AttributeRequest) -> Tuple[str, bytes]:
        """
        Build the payload for an attribute request response.
        :param request: The AttributeRequest to build the payload for.
        :return: A tuple of topic and payload bytes.
        """
        if not request.request_id:
            raise ValueError("AttributeRequest must have a valid ID.")

        topic = mqtt_topics.build_device_attributes_request_topic(request.request_id)
        payload = dumps(request.to_payload_format())
        logger.trace("Built attribute request payload for request: %r", request)
        return topic, payload

    def build_claim_request(self, claim_request) -> Tuple[str, bytes]:
        """
        Build the payload for a claim request.
        :param claim_request: The ClaimRequest to build the payload for.
        :return: A tuple of topic and payload bytes.
        """
        if not claim_request.secret_key:
            raise ValueError("ClaimRequest must have a valid secret key.")

        topic = mqtt_topics.DEVICE_CLAIM_TOPIC
        payload = dumps(claim_request.to_payload_format())
        logger.trace("Built claim request payload: %r", claim_request)
        return topic, payload

    def build_rpc_request(self, rpc_request: RPCRequest) -> Tuple[str, bytes]:
        """
        Build the payload for an RPC request.
        :param rpc_request: The RPC request to build the payload for.
        :return: A tuple of topic and payload bytes.
        """
        if not rpc_request.request_id:
            raise ValueError("RPCRequest must have a valid ID.")

        payload = dumps(rpc_request.to_payload_format())
        topic = mqtt_topics.DEVICE_RPC_REQUEST_TOPIC + str(rpc_request.request_id)
        logger.trace("Built RPC request payload for request ID=%d with payload: %r",
                     rpc_request.request_id, payload)
        return topic, payload

    def build_rpc_response(self, rpc_response: RPCResponse) -> Tuple[str, bytes]:
        """
        Build the payload for an RPC response.
        :param rpc_response: The RPC response to build the payload for.
        :return: A tuple of topic and payload bytes.
        """
        if not rpc_response.request_id:
            raise ValueError("RPCResponse must have a valid request ID.")

        payload = dumps(rpc_response.to_payload_format())
        topic = mqtt_topics.DEVICE_RPC_RESPONSE_TOPIC + str(rpc_response.request_id)
        logger.trace("Built RPC response payload for request ID=%d with payload: %r", rpc_response.request_id, payload)
        return topic, payload

    def build_provision_request(self, provision_request: 'ProvisioningRequest') -> Tuple[str, bytes]:
        """
        Build the payload for a device provisioning request.
        :param provision_request: The ProvisioningRequest to build the payload for.
        :return: A tuple of topic and payload bytes.
        """
        if not provision_request.credentials.provision_device_key or not provision_request.credentials.provision_device_secret:
            raise ValueError("ProvisioningRequest must have valid device key and secret.")

        topic = mqtt_topics.PROVISION_REQUEST_TOPIC
        request = {}
        request["provisionDeviceKey"] = provision_request.credentials.provision_device_key
        request["provisionDeviceSecret"] = provision_request.credentials.provision_device_secret

        if provision_request.device_name:
            request["deviceName"] = provision_request.device_name

        if provision_request.gateway:
            request["gateway"] = provision_request.gateway

        if provision_request.credentials.credentials_type and \
                provision_request.credentials.credentials_type == ProvisioningCredentialsType.ACCESS_TOKEN:
            if provision_request.credentials.access_token is not None:
                request["token"] = provision_request.credentials.access_token
            request["credentialsType"] = provision_request.credentials.credentials_type.value

        if provision_request.credentials.credentials_type == ProvisioningCredentialsType.MQTT_BASIC:
            if provision_request.credentials.username is not None:
                request["username"] = provision_request.credentials.username

            if provision_request.credentials.password is not None:
                request["password"] = provision_request.credentials.password

            if provision_request.credentials.client_id is not None:
                request["clientId"] = provision_request.credentials.client_id

            request["credentialsType"] = provision_request.credentials.credentials_type.value

        if provision_request.credentials.credentials_type == ProvisioningCredentialsType.X509_CERTIFICATE:
            request["hash"] = provision_request.credentials.public_cert
            request["credentialsType"] = provision_request.credentials.credentials_type.value

        payload = dumps(request)
        logger.trace("Built provision request payload: %r", provision_request)
        return topic, payload

    @staticmethod
    def build_payload(msg: DeviceUplinkMessage, build_timeseries_payload) -> bytes:
        result: Union[Dict[str, Any], List[Dict[str, Any]]] = {}
        if build_timeseries_payload:
            logger.trace("Packing timeseries")
            result = JsonMessageAdapter.pack_timeseries(msg)
        else:
            logger.trace("Packing attributes")
            result = JsonMessageAdapter.pack_attributes(msg)

        payload = dumps(result)
        logger.trace("Built payload size: %d bytes", len(payload))
        return payload

    @staticmethod
    def pack_attributes(msg: DeviceUplinkMessage) -> Dict[str, Any]:
        logger.trace("Packing %d attribute(s)", len(msg.attributes))
        return {attr.key: attr.value for attr in msg.attributes}

    @staticmethod
    def pack_timeseries(msg: 'DeviceUplinkMessage') -> List[Dict[str, Any]]:
        now_ts = int(datetime.now(UTC).timestamp() * 1000)
        if all(entry.ts is None for entry in chain.from_iterable(msg.timeseries.values())):
            packed = {
                "ts": now_ts,
                "values": {entry.key: entry.value for entry in chain.from_iterable(msg.timeseries.values())}
            }
        else:
            packed = [
                {"ts": entry.ts or now_ts, "values": {entry.key: entry.value}}
                for entry in chain.from_iterable(msg.timeseries.values())
            ]

        return packed
