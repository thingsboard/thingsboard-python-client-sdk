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


from abc import ABC, abstractmethod
from typing import Any, Dict, Union, List, Tuple, Optional
from orjson import dumps

from tb_mqtt_client.constants.mqtt_topics import DEVICE_TELEMETRY_TOPIC, DEVICE_ATTRIBUTES_TOPIC
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage
from tb_mqtt_client.service.message_splitter import MessageSplitter
from tb_mqtt_client.common.logging_utils import get_logger

logger = get_logger(__name__)


class MessageDispatcher(ABC):
    def __init__(self, max_payload_size: Optional[int] = None, max_datapoints: Optional[int] = None):
        self._splitter = MessageSplitter(max_payload_size, max_datapoints)
        logger.trace("MessageDispatcher initialized with max_payload_size=%s, max_datapoints=%s",
                     max_payload_size, max_datapoints)

    @abstractmethod
    def build_topic_payloads(
        self,
        messages: List[DeviceUplinkMessage]
    ) -> List[Tuple[str, bytes, int]]:
        """
        Build a list of topic-payload pairs from the given messages.
        Each pair consists of a topic string and a payload byte array.
        """
        pass

    @abstractmethod
    def build_payload(self, msg: DeviceUplinkMessage) -> bytes:
        """
        Build a JSON payload for a single DeviceUplinkMessage.
        """
        pass

    @abstractmethod
    def splitter(self) -> MessageSplitter:
        """
        Get the message splitter instance.
        """
        pass


class JsonMessageDispatcher(MessageDispatcher):
    def __init__(self, max_payload_size: Optional[int] = None, max_datapoints: Optional[int] = None):
        super().__init__(max_payload_size, max_datapoints)
        logger.trace("JsonMessageDispatcher created.")

    @property
    def splitter(self) -> MessageSplitter:
        return self._splitter

    def build_topic_payloads(self, messages: List[DeviceUplinkMessage]) -> List[Tuple[str, bytes, int]]:
        if not messages:
            logger.trace("No messages to process in build_topic_payloads.")
            return []

        from collections import defaultdict

        result: List[Tuple[str, bytes, int]] = []
        device_groups: Dict[str, List[DeviceUplinkMessage]] = defaultdict(list)

        for msg in messages:
            device_name = msg.device_name or "<unnamed>"
            device_groups[device_name].append(msg)
            logger.trace("Queued message for device='%s'", device_name)

        logger.trace("Processing %d device group(s).", len(device_groups))

        for device, device_msgs in device_groups.items():
            telemetry_msgs = [m for m in device_msgs if m.has_timeseries()]
            attr_msgs = [m for m in device_msgs if m.has_attributes()]
            logger.trace("Device '%s' - telemetry: %d, attributes: %d",
                         device, len(telemetry_msgs), len(attr_msgs))

            for ts_batch in self._splitter.split_timeseries(telemetry_msgs):
                payload = self.build_payload(ts_batch)
                count = ts_batch.timeseries_datapoint_count()
                result.append((DEVICE_TELEMETRY_TOPIC, payload, count))
                logger.trace("Built telemetry payload for device='%s' with %d datapoints", device, count)

            for attr_batch in self._splitter.split_attributes(attr_msgs):
                payload = self.build_payload(attr_batch)
                count = len(attr_batch.attributes)
                result.append((DEVICE_ATTRIBUTES_TOPIC, payload, count))
                logger.trace("Built attribute payload for device='%s' with %d attributes", device, count)

        logger.trace("Generated %d topic-payload entries.", len(result))
        return result

    def build_payload(self, msg: DeviceUplinkMessage) -> bytes:
        result: Dict[str, Any] = {}
        device_name = msg.device_name or "<unnamed>"
        logger.trace("Building payload for device='%s'", device_name)

        if msg.device_name:
            if msg.attributes:
                logger.trace("Packing attributes for device='%s'", device_name)
                result[msg.device_name] = self._pack_attributes(msg)
            if msg.timeseries:
                logger.trace("Packing timeseries for device='%s'", device_name)
                result[msg.device_name] = self._pack_timeseries(msg)
        else:
            if msg.attributes:
                logger.trace("Packing anonymous attributes")
                result = self._pack_attributes(msg)
            if msg.timeseries:
                logger.trace("Packing anonymous timeseries")
                result = self._pack_timeseries(msg)

        payload = dumps(result)
        logger.trace("Built payload size: %d bytes", len(payload))
        return payload

    @staticmethod
    def _pack_attributes(msg: DeviceUplinkMessage) -> Dict[str, Any]:
        logger.trace("Packing %d attribute(s)", len(msg.attributes))
        return {attr.key: attr.value for attr in msg.attributes}

    @staticmethod
    def _pack_timeseries(msg: DeviceUplinkMessage) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        logger.trace("Packing %d timeseries entry(ies)", len(msg.timeseries))
        grouped = {}
        for entry in msg.timeseries:
            grouped.setdefault(entry.ts or 0, {})[entry.key] = entry.value

        if all(ts == 0 for ts in grouped):
            return grouped[0]
        return [{"ts": ts, "values": values} for ts, values in grouped.items()]
