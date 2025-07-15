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
from typing import List, Optional, Dict, Tuple

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.device_uplink_message import GatewayUplinkMessage, DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry

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

    def split_timeseries(self, messages: List[GatewayUplinkMessage]) -> List[GatewayUplinkMessage]:
        logger.trace("Splitting timeseries for %d messages", len(messages))

        if (len(messages) == 1
                and ((messages[0].attributes_datapoint_count() + messages[
                    0].timeseries_datapoint_count() <= self._max_datapoints) or self._max_datapoints == 0)  # noqa
                and messages[0].size <= self._max_payload_size):
            return messages

        result: List[GatewayUplinkMessage] = []

        grouped: Dict[Tuple[str, Optional[str]], List[GatewayUplinkMessage]] = defaultdict(list)
        for msg in messages:
            key = (msg.device_name, msg.device_profile)
            grouped[key].append(msg)

        for (device_name, device_profile), group_msgs in grouped.items():
            logger.trace("Processing group: device='%s', profile='%s', messages=%d", device_name, device_profile,
                         len(group_msgs))

            all_ts: List[TimeseriesEntry] = []
            delivery_futures: List[asyncio.Future] = []
            for msg in group_msgs:
                if msg.has_timeseries():
                    for ts_group in msg.timeseries.values():
                        all_ts.extend(ts_group)
                delivery_futures.extend(msg.get_delivery_futures())

            builder: Optional[DeviceUplinkMessageBuilder] = None
            size = 0
            point_count = 0
            batch_futures = []

            for ts_kv in all_ts:
                exceeds_size = builder and size + ts_kv.size > self._max_payload_size
                exceeds_points = 0 < self._max_datapoints <= point_count

                if not builder or exceeds_size or exceeds_points:
                    if builder:
                        built = builder.build()
                        result.append(built)
                        batch_futures.extend(built.get_delivery_futures())
                        logger.trace("Flushed batch with %d points (size=%d)", len(built.timeseries), size)

                    builder = DeviceUplinkMessageBuilder() \
                        .set_device_name(device_name) \
                        .set_device_profile(device_profile)
                    size = 0
                    point_count = 0

                builder.add_timeseries(ts_kv)
                size += ts_kv.size
                point_count += 1

            if builder and builder._timeseries:  # noqa
                built = builder.build()
                result.append(built)
                batch_futures.extend(built.get_delivery_futures())
                logger.trace("Flushed final batch with %d points (size=%d)", len(built.timeseries), size)

            if delivery_futures:
                original_future = delivery_futures[0]
                logger.trace("Adding futures to original future: %s, futures ids: %r", id(original_future),
                             [id(f) for f in batch_futures])

                async def resolve_original():
                    logger.trace("Resolving original future with batch futures: %r", [id(f) for f in batch_futures])
                    results = await asyncio.gather(*batch_futures, return_exceptions=False)
                    original_future.set_result(all(results))

                asyncio.create_task(resolve_original())

        logger.trace("Total timeseries batches created: %d", len(result))
        return result

    def split_attributes(self, messages: List[GatewayUplinkMessage]) -> List[GatewayUplinkMessage]:
        logger.trace("Splitting attributes for %d messages", len(messages))
        result: List[GatewayUplinkMessage] = []

        if (len(messages) == 1
                and ((messages[0].attributes_datapoint_count() + messages[
                    0].timeseries_datapoint_count() <= self._max_datapoints) or self._max_datapoints == 0)  # noqa
                and messages[0].size <= self._max_payload_size):
            return messages

        grouped: Dict[Tuple[str, Optional[str]], List[GatewayUplinkMessage]] = defaultdict(list)
        for msg in messages:
            grouped[(msg.device_name, msg.device_profile)].append(msg)

        for (device_name, device_profile), group_msgs in grouped.items():
            logger.trace("Processing attribute group: device='%s', profile='%s', messages=%d", device_name,
                         device_profile, len(group_msgs))

            all_attrs: List[AttributeEntry] = []
            delivery_futures: List[asyncio.Future] = []

            for msg in group_msgs:
                if msg.has_attributes():
                    all_attrs.extend(msg.attributes)
                delivery_futures.extend(msg.get_delivery_futures())

            builder = None
            size = 0
            point_count = 0
            batch_futures = []

            for attr in all_attrs:
                exceeds_size = builder and size + attr.size > self._max_payload_size
                exceeds_points = 0 < self._max_datapoints <= point_count

                if not builder or exceeds_size or exceeds_points:
                    if builder:
                        built = builder.build()
                        result.append(built)
                        batch_futures.extend(built.get_delivery_futures())
                        logger.trace("Flushed attribute batch (count=%d, size=%d)", len(built.attributes), size)
                    builder = DeviceUplinkMessageBuilder().set_device_name(device_name).set_device_profile(
                        device_profile)
                    size = 0
                    point_count = 0

                builder.add_attributes(attr)
                size += attr.size
                point_count += 1

            if builder and builder._attributes:  # noqa
                built = builder.build()
                result.append(built)
                batch_futures.extend(built.get_delivery_futures())
                logger.trace("Flushed final attribute batch (count=%d, size=%d)", len(built.attributes), size)

            if delivery_futures:
                original_future = delivery_futures[0]
                logger.trace("Adding futures to original future: %s, futures ids: %r", id(original_future),
                             [id(batch_future) for batch_future in batch_futures])

                async def resolve_original():
                    results = await asyncio.gather(*batch_futures, return_exceptions=False)
                    original_future.set_result(all(results))

                asyncio.create_task(resolve_original())

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
