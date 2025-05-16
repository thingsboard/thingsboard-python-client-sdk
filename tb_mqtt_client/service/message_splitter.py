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


from typing import List
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessage, DeviceUplinkMessageBuilder
from tb_mqtt_client.common.logging_utils import get_logger

logger = get_logger(__name__)


class MessageSplitter:
    def __init__(self, max_payload_size: int = 65535, max_datapoints: int = 0):
        if max_payload_size is None or max_payload_size <= 0:
            logger.debug("Invalid max_payload_size: %s, using default 65535", max_payload_size)
            max_payload_size = 65535
        if max_datapoints is None or max_datapoints < 0:
            logger.debug("Invalid max_datapoints: %s, using default 0", max_datapoints)
            max_datapoints = 0

        self._max_payload_size = max_payload_size
        self._max_datapoints = max_datapoints
        logger.trace("MessageSplitter initialized with max_payload_size=%d, max_datapoints=%d",
                     self._max_payload_size, self._max_datapoints)

    def split_timeseries(self, messages: List[DeviceUplinkMessage]) -> List[DeviceUplinkMessage]:
        logger.trace("Splitting timeseries for %d messages", len(messages))
        result: List[DeviceUplinkMessage] = []

        for message in messages:
            if not message.has_timeseries():
                logger.trace("Message from device '%s' has no timeseries. Skipping.", message.device_name)
                continue

            logger.trace("Processing timeseries from device: %s", message.device_name)
            builder = None
            size = 0
            point_count = 0

            for ts in message.timeseries:
                exceeds_size = builder and size + ts.size > self._max_payload_size
                exceeds_points = self._max_datapoints > 0 and point_count >= self._max_datapoints

                if not builder or exceeds_size or exceeds_points:
                    if builder:
                        built = builder.build()
                        result.append(built)
                        logger.trace("Flushed batch with %d points (size=%d)", len(built.timeseries), size)
                    builder = DeviceUplinkMessageBuilder() \
                        .set_device_name(message.device_name) \
                        .set_device_profile(message.device_profile)
                    size = 0
                    point_count = 0

                builder.add_telemetry(ts)
                size += ts.size
                point_count += 1
                logger.trace("Added timeseries entry to batch (size=%d, points=%d)", size, point_count)

            if builder and builder._timeseries:
                built = builder.build()
                result.append(built)
                logger.trace("Flushed final batch with %d points (size=%d)", len(built.timeseries), size)

        logger.trace("Total timeseries batches created: %d", len(result))
        return result

    def split_attributes(self, messages: List[DeviceUplinkMessage]) -> List[DeviceUplinkMessage]:
        logger.trace("Splitting attributes for %d messages", len(messages))
        result: List[DeviceUplinkMessage] = []

        for message in messages:
            if not message.has_attributes():
                logger.trace("Message from device '%s' has no attributes. Skipping.", message.device_name)
                continue

            logger.trace("Processing attributes from device: %s", message.device_name)
            builder = None
            size = 0

            for attr in message.attributes:
                if builder and size + attr.size > self._max_payload_size:
                    built = builder.build()
                    result.append(built)
                    logger.trace("Flushed attribute batch (count=%d, size=%d)", len(built.attributes), size)
                    builder = None
                    size = 0

                if not builder:
                    builder = DeviceUplinkMessageBuilder() \
                        .set_device_name(message.device_name) \
                        .set_device_profile(message.device_profile)

                builder.add_attributes(attr)
                size += attr.size
                logger.trace("Added attribute to batch (size=%d)", size)

            if builder and builder._attributes:
                built = builder.build()
                result.append(built)
                logger.trace("Flushed final attribute batch (count=%d, size=%d)", len(built.attributes), size)

        logger.trace("Total attribute batches created: %d", len(result))
        return result

    @property
    def max_payload_size(self) -> int:
        return self._max_payload_size

    @max_payload_size.setter
    def max_payload_size(self, value: int):
        old = self._max_payload_size
        self._max_payload_size = value if value > 0 else 65535
        logger.debug("Updated max_payload_size: %d -> %d", old, self._max_payload_size)

    @property
    def max_datapoints(self) -> int:
        return self._max_datapoints

    @max_datapoints.setter
    def max_datapoints(self, value: int):
        old = self._max_datapoints
        self._max_datapoints = value if value > 0 else 0
        logger.debug("Updated max_datapoints: %d -> %d", old, self._max_datapoints)
