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
from abc import ABC, abstractmethod
from typing import Callable, Awaitable, Dict, Any, Union, List, Optional

import uvloop

from tb_mqtt_client.common.exceptions import exception_handler
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.common.publish_result import PublishResult

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
exception_handler.install_asyncio_handler()


class BaseClient(ABC):
    """
    Abstract base class for clients.
    """

    DEFAULT_TIMEOUT = 3.0

    def __init__(self, host: str, port: int, client_id: str):
        self._host = host
        self._port = port
        self._client_id = client_id
        self._connected = asyncio.Event()

    @abstractmethod
    async def connect(self):
        """
        Connect to the platform over MQTT.
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """
        Disconnect from the platform.
        """
        pass

    @abstractmethod
    async def send_timeseries(self,
                              telemetry_data: Union[TimeseriesEntry,
                              List[TimeseriesEntry],
                              Dict[str, Any],
                              List[Dict[str, Any]]],
                              wait_for_publish: bool = True,
                              timeout: Optional[float] = None) -> Union[asyncio.Future[PublishResult],
                                                                        PublishResult,
                                                                        None,
                                                                        List[PublishResult],
                                                                        List[asyncio.Future[PublishResult]]]:
        """
        Sends timeseries data to the ThingsBoard server.
        :param data: Timeseries data to send, can be a single TimeseriesEntry, a list of TimeseriesEntries,
                     a dictionary of key-value pairs, or a list of dictionaries.
        :param qos: Quality of Service level for the MQTT message.
        :param wait_for_publish: If True, waits for the publish result.
        :param timeout: Timeout for waiting for the publish result.
        :return: PublishResult or list of PublishResults if wait_for_publish is True, Future or list of Futures if not,
                    None if no data is sent.
        """
        pass

    @abstractmethod
    async def send_attributes(self,
                              attributes: Union[Dict[str, Any], AttributeEntry, list[AttributeEntry]],
                              wait_for_publish: bool = True,
                              timeout: Optional[float] = None) -> Union[asyncio.Future[PublishResult], PublishResult]:
        """
        Send client attributes.

        :param attributes: Dictionary of attributes or a single AttributeEntry or a list of AttributeEntries.
        :param wait_for_publish: If True, wait for the publishing result. Default is True.
        :param timeout: Timeout for the publish operation if `wait_for_publish` is True.
                                In seconds. If less than 0 or None, wait indefinitely.
        :return: Future or PublishResult depending on `wait_for_publish`.
        """
        pass

    @abstractmethod
    async def claim_device(self, claim_request: ClaimRequest) -> Union[asyncio.Future[PublishResult], PublishResult]:
        """
        Claim a device using the provided ClaimRequest.

        :param claim_request: The ClaimRequest instance contains secret key and duration.
        :return: Future or PublishResult depending on the implementation.
        """
        pass

    @abstractmethod
    async def send_rpc_response(self, response: RPCResponse):
        """
        Send a response to a server-initiated RPC request.

        :param RPCResponse response: The RPC response for sending to the platform.
        """
        pass

    @abstractmethod
    def set_attribute_update_callback(self, callback: Callable[[AttributeUpdate], Awaitable[None]]):
        """
        Set callback to be triggered when a shared attribute update is received.

        :param callback: Coroutine accepting an AttributeUpdate instance.
        """
        pass

    @abstractmethod
    def set_rpc_request_callback(self, callback: Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        """
        Set callback to be triggered when an RPC request is received.

        :param callback: Coroutine accepting (method, params) and returning result.
        """
        pass