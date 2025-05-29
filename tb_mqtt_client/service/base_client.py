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
from typing import Callable, Awaitable, Dict, Any, Union

import uvloop

from tb_mqtt_client.common.exceptions import exception_handler
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.rpc_response import RPCResponse

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
exception_handler.install_asyncio_handler()


class BaseClient(ABC):
    """
    Abstract base class for ThingsBoard clients.
    """

    def __init__(self, host: str, port: int, client_id: str):
        self._host = host
        self._port = port
        self._client_id = client_id
        self._connected = asyncio.Event()

    @abstractmethod
    async def connect(self):
        """
        Connect to the ThingsBoard platform over MQTT.
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """
        Disconnect from the platform.
        """
        pass

    @abstractmethod
    async def send_telemetry(self, telemetry_data: Dict[str, Any]):
        """
        Send telemetry data.

        :param telemetry_data: Dictionary of telemetry key-values.
        """
        pass

    @abstractmethod
    async def send_attributes(self, attributes: Union[Dict[str, Any], AttributeEntry, list[AttributeEntry]]):
        """
        Send client-side attributes.

        :param attributes: Dictionary of attributes.
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
