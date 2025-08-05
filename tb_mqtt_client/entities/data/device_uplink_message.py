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
from dataclasses import dataclass
from types import MappingProxyType
from typing import List, Optional, Union, OrderedDict, Tuple, Mapping
from uuid import uuid4

from tb_mqtt_client.common.logging_utils import get_logger, TRACE_LEVEL
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry

logger = get_logger(__name__)

DEFAULT_FIELDS_SIZE = len('{"device_name":"","device_profile":"","attributes":"","timeseries":""}'.encode('utf-8'))


@dataclass(slots=True, frozen=True)
class DeviceUplinkMessage:
    device_name: Optional[str]
    device_profile: Optional[str]
    attributes: Tuple[AttributeEntry]
    timeseries: Mapping[int, Tuple[TimeseriesEntry]]
    delivery_futures: List[Optional[asyncio.Future[PublishResult]]]
    _size: int
    main_ts: Optional[int] = None

    def __new__(cls, *args, **kwargs):
        raise TypeError(
            "Direct instantiation of DeviceUplinkMessage is not allowed. "
            "Use DeviceUplinkMessageBuilder to construct instances.")

    def __repr__(self):
        return (f"DeviceUplinkMessage(device_name={self.device_name}, "
                f"device_profile={self.device_profile}, "
                f"attributes={self.attributes}, "
                f"timeseries={self.timeseries}, "
                f"delivery_futures={self.delivery_futures})")

    @classmethod
    def build(cls,
              device_name: Optional[str],
              device_profile: Optional[str],
              attributes: List[AttributeEntry],
              timeseries: Mapping[int, List[TimeseriesEntry]],
              delivery_futures: List[Optional[asyncio.Future]],
              size: int,
              main_ts: Optional[int]) -> 'DeviceUplinkMessage':
        self = object.__new__(cls)
        object.__setattr__(self, 'device_name', device_name)
        object.__setattr__(self, 'device_profile', device_profile)
        object.__setattr__(self, 'attributes', tuple(attributes))
        object.__setattr__(self, 'timeseries',
                           MappingProxyType({ts: tuple(entries) for ts, entries in timeseries.items()}))
        object.__setattr__(self, 'delivery_futures', tuple(delivery_futures))
        object.__setattr__(self, '_size', size)
        object.__setattr__(self, 'main_ts', main_ts)
        return self

    @property
    def size(self) -> int:
        return self._size

    def timeseries_datapoint_count(self) -> int:
        return sum(len(entries) for entries in self.timeseries.values())

    def attributes_datapoint_count(self) -> int:
        return len(self.attributes)

    def has_attributes(self) -> bool:
        return bool(self.attributes)

    def has_timeseries(self) -> bool:
        return bool(self.timeseries)

    def get_delivery_futures(self) -> List[Optional[asyncio.Future[PublishResult]]]:
        return self.delivery_futures

    def set_main_ts(self, main_ts: int) -> 'DeviceUplinkMessage':
        """
        Set the main timestamp for the message.
        :param main_ts: The main timestamp to set.
        :return: The updated DeviceUplinkMessage instance.
        """
        object.__setattr__(self, 'main_ts', main_ts)
        return self


class DeviceUplinkMessageBuilder:
    def __init__(self):
        self._device_name: Optional[str] = None
        self._device_profile: Optional[str] = None
        self._attributes: List[AttributeEntry] = []
        self._timeseries: OrderedDict[int, List[TimeseriesEntry]] = OrderedDict()
        self._delivery_futures: List[Optional[asyncio.Future[PublishResult]]] = []
        self.__size = DEFAULT_FIELDS_SIZE
        self._main_ts: Optional[int] = None

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

    def add_timeseries(self,
                       timeseries: Union[TimeseriesEntry,
                                         List[TimeseriesEntry],
                                         OrderedDict[int, List[TimeseriesEntry]]]) -> 'DeviceUplinkMessageBuilder':
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

    def add_delivery_futures(self, futures: Union[asyncio.Future[PublishResult],
                                                  List[asyncio.Future[PublishResult]]]) -> 'DeviceUplinkMessageBuilder':
        if not isinstance(futures, list):
            futures = [futures]
        if futures:
            if logger.isEnabledFor(TRACE_LEVEL):
                logger.debug("Created delivery futures: %r", [future.uuid for future in futures])
            self._delivery_futures.extend(futures)
        return self

    def set_main_ts(self, main_ts: int) -> 'DeviceUplinkMessageBuilder':
        self._main_ts = main_ts
        return self

    def build(self) -> DeviceUplinkMessage:
        if not self._delivery_futures:
            delivery_future = asyncio.get_event_loop().create_future()
            delivery_future.uuid = uuid4()
            logger.trace("No delivery futures provided, creating a default future: %r", delivery_future.uuid)
            self._delivery_futures = [delivery_future]
        return DeviceUplinkMessage.build(
            device_name=self._device_name,
            device_profile=self._device_profile,
            attributes=self._attributes,
            timeseries=self._timeseries,
            delivery_futures=self._delivery_futures,
            size=self.__size,
            main_ts=self._main_ts
        )
