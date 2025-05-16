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


from typing import List, Optional, Union
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry

DEFAULT_FIELDS_SIZE = len('{"device_name":"","device_profile":"","attributes":"","timeseries":""}'.encode('utf-8'))


class DeviceUplinkMessage:
    def __init__(self,
                 device_name: Optional[str] = None,
                 device_profile: Optional[str] = None,
                 attributes: Optional[List[AttributeEntry]] = None,
                 timeseries: Optional[List[TimeseriesEntry]] = None,
                 _size: Optional[int] = None):
        if _size is None:
            raise ValueError("DeviceUplinkMessage must be created using DeviceUplinkMessageBuilder")

        self.device_name = device_name
        self.device_profile = device_profile
        self.attributes = attributes or []
        self.timeseries = timeseries or []
        self.__size = _size

    def timeseries_datapoint_count(self) -> int:
        return len(self.timeseries)

    def has_attributes(self) -> bool:
        return bool(self.attributes)

    def has_timeseries(self) -> bool:
        return bool(self.timeseries)

    @property
    def size(self) -> int:
        return self.__size


class DeviceUplinkMessageBuilder:
    def __init__(self):
        self._device_name: Optional[str] = None
        self._device_profile: Optional[str] = None
        self._attributes: List[AttributeEntry] = []
        self._timeseries: List[TimeseriesEntry] = []
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

    def add_telemetry(self, timeseries: Union[TimeseriesEntry, List[TimeseriesEntry]]) -> 'DeviceUplinkMessageBuilder':
        if not isinstance(timeseries, list):
            timeseries = [timeseries]
        self._timeseries.extend(timeseries)
        for timeseries_entry in timeseries:
            self.__size += timeseries_entry.size
        return self

    def build(self) -> DeviceUplinkMessage:
        return DeviceUplinkMessage(
            device_name=self._device_name,
            device_profile=self._device_profile,
            attributes=self._attributes,
            timeseries=self._timeseries,
            _size=self.__size
        )
