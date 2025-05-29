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
from typing import List, Optional, Union, OrderedDict

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.publish_result import PublishResult

logger = get_logger(__name__)

DEFAULT_FIELDS_SIZE = len('{"device_name":"","device_profile":"","attributes":"","timeseries":""}'.encode('utf-8'))


class DeviceUplinkMessage:
    def __init__(self,
                 device_name: Optional[str] = None,
                 device_profile: Optional[str] = None,
                 attributes: Optional[List[AttributeEntry]] = None,
                 timeseries: Optional[OrderedDict[int, List[TimeseriesEntry]]] = None,
                 _size: Optional[int] = None,
                 delivery_future: List[Optional[asyncio.Future[PublishResult]]] = None):
        if _size is None:
            raise ValueError("DeviceUplinkMessage must be created using DeviceUplinkMessageBuilder")

        self.device_name = device_name
        self.device_profile = device_profile
        self.attributes = attributes or []
        self.timeseries = timeseries or []
        self.__size = _size
        self.delivery_futures = delivery_future or []


    def timeseries_datapoint_count(self) -> int:
        return len(self.timeseries)

    def attributes_datapoint_count(self) -> int:
        return len(self.attributes)

    def has_attributes(self) -> bool:
        return bool(self.attributes)

    def has_timeseries(self) -> bool:
        return bool(self.timeseries)

    def get_delivery_futures(self):
        return self.delivery_futures

    @property
    def size(self) -> int:
        return self.__size


class DeviceUplinkMessageBuilder:
    def __init__(self):
        self._device_name: Optional[str] = None
        self._device_profile: Optional[str] = None
        self._attributes: List[AttributeEntry] = []
        self._timeseries: OrderedDict[int, List[TimeseriesEntry]] = OrderedDict()
        self._delivery_futures: List[Optional[asyncio.Future[PublishResult]]] = []
        self.__size = DEFAULT_FIELDS_SIZE

    def set_device_name(self, device_name: str) -> 'DeviceUplinkMessageBuilder':
        self._device_name = device_name
        if device_name is not None:
            self.__size += len(device_name)
        return self

    def set_device_profile(self, profile: str) -> 'DeviceUplinkMessageBuilder':
        self._device_profile = profile
        if profile is not None:
            self.__size += len(profile)
        return self

    def add_attributes(self, attributes: Union[AttributeEntry, List[AttributeEntry]]) -> 'DeviceUplinkMessageBuilder':
        if not isinstance(attributes, list):
            attributes = [attributes]
        self._attributes.extend(attributes)
        for attribute in attributes:
            self.__size += attribute.size
        return self

    def add_telemetry(self, timeseries: Union[TimeseriesEntry, List[TimeseriesEntry], OrderedDict[int, List[TimeseriesEntry]]]) -> 'DeviceUplinkMessageBuilder':
        if isinstance(timeseries, OrderedDict):
            self._timeseries = timeseries
            return self
        if not isinstance(timeseries, list):
            timeseries = [timeseries]
        for entry in timeseries:
            if entry.ts is not None:
                if entry.ts in self._timeseries:
                    self._timeseries[entry.ts].append(entry)
                else:
                    self._timeseries[entry.ts] = [entry]
            else:
                if 0 in self._timeseries:
                    self._timeseries[0].append(entry)
                else:
                    self._timeseries[0] = [entry]
        for timeseries_entry in timeseries:
            self.__size += timeseries_entry.size
        return self

    def add_delivery_futures(self, futures: Union[asyncio.Future[PublishResult], List[asyncio.Future[PublishResult]]]) -> 'DeviceUplinkMessageBuilder':
        if not isinstance(futures, list):
            futures = [futures]
        if futures:
            logger.debug("Created delivery futures: %r", [id(future) for future in futures])
            self._delivery_futures.extend(futures)
        return self

    def build(self) -> DeviceUplinkMessage:
        if not self._delivery_futures:
            self._delivery_futures = [asyncio.get_event_loop().create_future()]
        return DeviceUplinkMessage(
            device_name=self._device_name,
            device_profile=self._device_profile,
            attributes=self._attributes,
            timeseries=self._timeseries,
            _size=self.__size,
            delivery_future=self._delivery_futures
        )
