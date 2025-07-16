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

from asyncio import Future
from typing import List, Union, Optional

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.device_disconnect_message import DeviceDisconnectMessage
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessage
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter
from tb_mqtt_client.service.message_queue import MessageQueue

logger = get_logger(__name__)


class GatewayMessageSender:
    """
    Class responsible for sending uplink messages from devices connected to the gateway to the platform.
    It handles the serialization of the message and sends to uplink message queue.
    """

    def __init__(self):
        self._message_queue: Optional[MessageQueue] = None
        self._message_adapter: Optional[GatewayMessageAdapter] = None

    async def send_uplink_message(self, message: GatewayUplinkMessage, qos=1) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Sends a list of uplink messages to the platform.

        :param message: List of GatewayUplinkMessage objects to be sent.
        :param qos: Quality of Service level for the MQTT message.
        :returns: List of PublishResult or Future[PublishResult] if successful, None if failed.
        """
        if self._message_queue is None:
            logger.error("Cannot send uplink messages. Message queue is not set, do you connected to the platform?")
            return None
        if not message.has_timeseries() and not message.has_attributes():
            logger.warning("Uplink message does not contain timeseries or attributes, nothing to send.")
            return None
        futures = []
        if message.has_timeseries():
            topic = mqtt_topics.GATEWAY_TELEMETRY_TOPIC
            timeseries_futures = await self._message_queue.publish(
                                topic=topic,
                                payload=message,
                                datapoints_count=message.timeseries_datapoint_count(),
                                qos=qos
                                )
            futures.extend(timeseries_futures)
        if message.has_attributes():
            topic = mqtt_topics.GATEWAY_ATTRIBUTES_TOPIC
            attributes_futures = await self._message_queue.publish(
                                topic=topic,
                                payload=message,
                                datapoints_count=message.attributes_datapoint_count(),
                                qos=qos
                                )
            futures.extend(attributes_futures)
        return futures

    async def send_device_connect(self, device_connect_message: DeviceConnectMessage, qos=1) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Sends a device connect message to the platform.

        :param device_connect_message: DeviceConnectMessage object containing the device connection details.
        :param qos: Quality of Service level for the MQTT message.
        :returns: List of PublishResult or Future[PublishResult] if successful, None if failed.
        """
        if self._message_queue is None:
            logger.error("Cannot send device connect message. Message queue is not set, do you connected to the platform?")
            return None
        topic, payload = self._message_adapter.build_device_connect_message_payload(device_connect_message=device_connect_message)
        return await self._message_queue.publish(
            topic=topic,
            payload=payload,
            datapoints_count=1,
            qos=qos
        )

    async def send_device_disconnect(self, device_disconnect_message: DeviceDisconnectMessage, qos=1) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Sends a device disconnect message to the platform.

        :param device_disconnect_message: DeviceDisconnectMessage object containing the device disconnection details.
        :param qos: Quality of Service level for the MQTT message.
        :returns: List of PublishResult or Future[PublishResult] if successful, None if failed.
        """
        if self._message_queue is None:
            logger.error("Cannot send device disconnect message. Message queue is not set, do you connected to the platform?")
            return None
        topic, payload = self._message_adapter.build_device_disconnect_message_payload(device_disconnect_message=device_disconnect_message)
        return await self._message_queue.publish(
            topic=topic,
            payload=payload,
            datapoints_count=1,
            qos=qos
        )

    async def send_attributes_request(self, attribute_request: GatewayAttributeRequest, qos=1) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Sends an attribute request message to the platform.

        :param attribute_request: GatewayAttributeRequest object containing the attributes to be requested.
        :param qos: Quality of Service level for the MQTT message.
        :returns: List of PublishResult or Future[PublishResult] if successful, None if failed.
        """
        if self._message_queue is None:
            logger.error("Cannot send attribute request. Message queue is not set, do you connected to the platform?")
            return None
        topic, payload = self._message_adapter.build_gateway_attribute_request_payload(attribute_request=attribute_request)
        return await self._message_queue.publish(
            topic=topic,
            payload=payload,
            datapoints_count=1,
            qos=qos
        )

    async def send_rpc_response(self, rpc_response: 'GatewayRPCResponse', qos=1) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]:
        """
        Sends an RPC response message to the platform.

        :param rpc_response: GatewayRPCResponse object containing the RPC response details.
        :param qos: Quality of Service level for the MQTT message.
        :returns: List of PublishResult or Future[PublishResult] if successful, None if failed.
        """
        if self._message_queue is None:
            logger.error("Cannot send RPC response. Message queue is not set, do you connected to the platform?")
            return None
        topic, payload = self._message_adapter.build_rpc_response_payload(rpc_response=rpc_response)
        return await self._message_queue.publish(
            topic=topic,
            payload=payload,
            datapoints_count=1,
            qos=qos
        )

    def set_message_queue(self, message_queue: MessageQueue):
        """
        Sets the message queue for sending uplink messages.

        :param message_queue: An instance of MessageQueue to be used for sending messages.
        """
        self._message_queue = message_queue

    def set_message_adapter(self, message_adapter: GatewayMessageAdapter):
        """
        Sets the message adapter for serializing uplink messages.

        :param message_adapter: An instance of GatewayMessageAdapter to be used for serializing messages.
        """
        self._message_adapter = message_adapter
