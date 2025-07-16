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
from asyncio import Future
from typing import Union, List, Tuple, Dict, Any

from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.service.base_client import BaseClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession


class GatewayClientInterface(BaseClient, ABC):

    @abstractmethod
    async def connect_device(self, device_name: str, device_profile: str, wait_for_publish: bool) -> \
            Tuple[DeviceSession, List[Union[PublishResult, Future[PublishResult]]]]: ...

    @abstractmethod
    async def disconnect_device(self, device_session: DeviceSession, wait_for_publish: bool) -> \
            List[Union[PublishResult, Future[PublishResult]]]: ...

    @abstractmethod
    async def send_device_timeseries(self,
                                     device_session: DeviceSession,
                                     data: Union[TimeseriesEntry, List[TimeseriesEntry], Dict[str, Any], List[Dict[str, Any]]],
                                     wait_for_publish: bool) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]: ...

    @abstractmethod
    async def send_device_attributes(self,
                                     device_session: DeviceSession,
                                     attributes: Union[Dict[str, Any], AttributeEntry, list[AttributeEntry]],
                                     wait_for_publish: bool) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]: ...

    @abstractmethod
    async def send_device_attributes_request(self,
                                             device_session: DeviceSession,
                                             attributes: Union[AttributeRequest, GatewayAttributeRequest],
                                             wait_for_publish: bool) -> Union[List[Union[PublishResult, Future[PublishResult]]], None]: ...
