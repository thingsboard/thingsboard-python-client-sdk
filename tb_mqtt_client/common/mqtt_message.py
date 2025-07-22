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
from typing import Union, List
from uuid import uuid4

from gmqtt import Message

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessage

logger = get_logger(__name__)


class MqttPublishMessage(Message):
    """
    A custom Publish MQTT message class that extends the gmqtt Message class.
    Contains additional information like datapoints, to avoid rate limits exceeding.
    """
    def __init__(self,
                 topic: str,
                 payload: Union[bytes, GatewayUplinkMessage, DeviceUplinkMessage],
                 qos: int = 1,
                 retain: bool = False,
                 datapoints: int = 0,
                 delivery_futures = None,
                 **kwargs):
        """
        Initialize the MqttMessage with topic, payload, QoS, retain flag, and datapoints.
        """
        self.prepared = False
        self.payload = payload
        if isinstance(payload, bytes):
            super().__init__(topic, payload, qos, retain)
        self.topic = topic
        self.qos = qos
        if self.qos < 0 or self.qos > 1:
            logger.warning(f"Invalid QoS {self.qos} for topic {topic}, using default QoS 1")
            self.qos = 1
        self.dup = False
        self.retain = retain
        self.message_id = None
        self.datapoints = datapoints
        self.properties = kwargs
        self._is_sent = False
        self.delivery_futures: List[Future] = delivery_futures
        if not delivery_futures:
            delivery_future = Future()
            delivery_future.uuid = uuid4()
            self.delivery_futures = [delivery_future]
        logger.trace(f"Created MqttMessage with topic: {topic}, payload type: {type(payload).__name__}, "
                    f"datapoints: {datapoints}, delivery_future id: {self.delivery_futures[0].uuid}")


    def mark_as_sent(self, message_id: int):
        """Mark the message as sent."""
        self.message_id = message_id
        self._is_sent = True

    def is_sent(self) -> bool:
        """Check if the message has been sent."""
        return self._is_sent
