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

from abc import ABC, abstractmethod
from typing import List

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage


class BaseMessageSplitter(ABC):
    """
    Base class for message splitters in the ThingsBoard MQTT client.
    """

    @property
    @abstractmethod
    def max_payload_size(self) -> int:
        """
        Returns the maximum payload size for messages.
        """
        pass

    @max_payload_size.setter
    @abstractmethod
    def max_payload_size(self, value: int) -> None:
        """
        Sets the maximum payload size for messages.
        """
        pass

    @property
    @abstractmethod
    def max_datapoints(self) -> int:
        """
        Returns the maximum number of datapoints allowed in a message.
        """
        pass

    @max_datapoints.setter
    @abstractmethod
    def max_datapoints(self, value: int) -> None:
        """
        Sets the maximum number of datapoints allowed in a message.
        """
        pass

    @abstractmethod
    def split_timeseries(self, *args, **kwargs) -> List[MqttPublishMessage]:
        """
        Splits timeseries data
        """
        pass

    @abstractmethod
    def split_attributes(self, *args, **kwargs) -> List[MqttPublishMessage]:
        """
        Splits attributes data
        """
        pass
