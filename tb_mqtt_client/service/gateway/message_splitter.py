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
from collections import defaultdict
from typing import List, Dict, Tuple
from uuid import uuid4

from tb_mqtt_client.common.async_utils import future_map
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessage, GatewayUplinkMessageBuilder, \
    DEFAULT_FIELDS_SIZE
from tb_mqtt_client.service.base_message_splitter import BaseMessageSplitter

logger = get_logger(__name__)


class GatewayMessageSplitter(BaseMessageSplitter):
    DEFAULT_MAX_PAYLOAD_SIZE = 55000  # Default max payload size in bytes

    def __init__(self, max_payload_size: int = 55000, max_datapoints: int = 0):
        self._max_payload_size = max_payload_size if max_payload_size is not None and max_payload_size > 0 else self.DEFAULT_MAX_PAYLOAD_SIZE
        self._max_payload_size = self._max_payload_size - DEFAULT_FIELDS_SIZE
        self._max_datapoints = max_datapoints if max_datapoints is not None and max_datapoints > 0 else 0
        logger.trace("GatewayMessageSplitter initialized with max_payload_size=%d, max_datapoints=%d",
                     self._max_payload_size, self._max_datapoints)

    def split_timeseries(self, messages: List[GatewayUplinkMessage]) -> List[GatewayUplinkMessage]:
        logger.trace("Splitting gateway timeseries for %d messages", len(messages))
        if (len(messages) == 1
                and ((messages[0].attributes_datapoint_count() + messages[0].timeseries_datapoint_count()
                      <= self._max_datapoints) or self._max_datapoints == 0)
                and messages[0].size <= self._max_payload_size):
            return messages

        result: List[GatewayUplinkMessage] = []
        grouped: Dict[Tuple[str, str], List[GatewayUplinkMessage]] = defaultdict(list)

        for msg in messages:
            grouped[(msg.device_name, msg.device_profile)].append(msg)

        for (device_name, device_profile), group_msgs in grouped.items():
            all_ts_entries: List[TimeseriesEntry] = []
            parent_futures: List[asyncio.Future] = []

            for msg in group_msgs:
                if msg.has_timeseries():
                    for ts_group in msg.timeseries.values():
                        all_ts_entries.extend(ts_group)
                parent_futures.extend(msg.get_delivery_futures() or [])

            builder = None
            size = 0
            point_count = 0
            names_len = len(device_name) + len(device_profile)

            for entry in all_ts_entries:
                exceeds_size = builder and size + entry.size > self._max_payload_size - names_len
                exceeds_points = 0 < self._max_datapoints <= point_count

                if not builder or exceeds_size or exceeds_points:
                    if builder:
                        shared_future = asyncio.get_running_loop().create_future()
                        shared_future.uuid = uuid4()
                        builder.add_delivery_futures(shared_future)

                        built = builder.build()
                        result.append(built)
                        for parent in parent_futures:
                            future_map.register(parent, [shared_future])
                        logger.trace("Flushed timeseries batch: size=%d, count=%d", size, point_count)

                    builder = GatewayUplinkMessageBuilder() \
                        .set_device_name(device_name) \
                        .set_device_profile(device_profile)
                    size = 0
                    point_count = 0

                builder.add_timeseries(entry)
                size += entry.size
                point_count += 1

            if builder and builder._timeseries:  # noqa
                shared_future = asyncio.get_running_loop().create_future()
                shared_future.uuid = uuid4()
                builder.add_delivery_futures(shared_future)
                built = builder.build()
                result.append(built)
                for parent in parent_futures:
                    future_map.register(parent, [shared_future])
                logger.trace("Flushed final timeseries batch: size=%d, count=%d", size, point_count)

        return result

    def split_attributes(self, messages: List[GatewayUplinkMessage]) -> List[GatewayUplinkMessage]:
        logger.trace("Splitting gateway attributes for %d messages", len(messages))
        if (len(messages) == 1
                and ((messages[0].attributes_datapoint_count() + messages[0].timeseries_datapoint_count()
                      <= self._max_datapoints) or self._max_datapoints == 0)
                and messages[0].size <= self._max_payload_size):
            return messages

        result: List[GatewayUplinkMessage] = []
        grouped: Dict[Tuple[str, str], List[GatewayUplinkMessage]] = defaultdict(list)

        for msg in messages:
            grouped[(msg.device_name, msg.device_profile)].append(msg)

        for (device_name, device_profile), group_msgs in grouped.items():
            all_attrs: List[AttributeEntry] = []
            parent_futures: List[asyncio.Future] = []

            for msg in group_msgs:
                if msg.has_attributes():
                    all_attrs.extend(msg.attributes)
                parent_futures.extend(msg.get_delivery_futures() or [])

            builder = None
            size = 0
            point_count = 0

            for attr in all_attrs:
                exceeds_size = builder and size + attr.size > self._max_payload_size
                exceeds_points = 0 < self._max_datapoints <= point_count

                if not builder or exceeds_size or exceeds_points:
                    if builder and builder._attributes:  # noqa
                        shared_future = asyncio.get_running_loop().create_future()
                        shared_future.uuid = uuid4()
                        builder.add_delivery_futures(shared_future)

                        built = builder.build()
                        result.append(built)
                        for parent in parent_futures:
                            future_map.register(parent, [shared_future])
                        logger.trace("Flushed attribute batch: size=%d, count=%d", size, point_count)

                    builder = GatewayUplinkMessageBuilder() \
                        .set_device_name(device_name) \
                        .set_device_profile(device_profile)
                    size = 0
                    point_count = 0

                builder.add_attributes(attr)
                size += attr.size
                point_count += 1

            if builder and builder._attributes:  # noqa
                shared_future = asyncio.get_running_loop().create_future()
                shared_future.uuid = uuid4()
                builder.add_delivery_futures(shared_future)

                built = builder.build()
                result.append(built)
                for parent in parent_futures:
                    future_map.register(parent, [shared_future])
                logger.trace("Flushed final attribute batch: size=%d, count=%d", size, point_count)

        return result

    @property
    def max_payload_size(self) -> int:
        return self._max_payload_size

    @max_payload_size.setter
    def max_payload_size(self, value: int):
        old = self._max_payload_size
        self._max_payload_size = value if value > 0 else self.DEFAULT_MAX_PAYLOAD_SIZE
        self._max_payload_size = self._max_payload_size - DEFAULT_FIELDS_SIZE
        logger.debug("Updated max_payload_size: %d -> %d", old, self._max_payload_size)

    @property
    def max_datapoints(self) -> int:
        return self._max_datapoints

    @max_datapoints.setter
    def max_datapoints(self, value: int):
        old = self._max_datapoints
        self._max_datapoints = value if value > 0 else 0
        logger.debug("Updated max_datapoints: %d -> %d", old, self._max_datapoints)