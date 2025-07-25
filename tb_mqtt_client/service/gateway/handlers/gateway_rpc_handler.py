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
from typing import Awaitable, Callable, Optional

from tb_mqtt_client.common.async_utils import await_or_stop
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.direct_event_dispatcher import DirectEventDispatcher
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter

logger = get_logger(__name__)


class GatewayRPCHandler:
    """
    Handles incoming RPC request messages for a device connected through the gateway.
    """

    def __init__(self, event_dispatcher: DirectEventDispatcher,
                 message_adapter: GatewayMessageAdapter,
                 device_manager: DeviceManager,
                 stop_event: asyncio.Event):
        self._event_dispatcher = event_dispatcher
        self._message_adapter = message_adapter
        self._device_manager = device_manager
        self._stop_event = stop_event
        self._callback: Optional[Callable[[GatewayRPCRequest], Awaitable[GatewayRPCResponse]]] = None
        self._event_dispatcher.register(GatewayEventType.DEVICE_RPC_REQUEST, self.handle)

    async def handle(self, topic: str, payload: bytes) -> None:
        """
        Process the RPC request and return the response for it.
        :returns: GatewayRPCResponse or None if failed
        """

        if not self._message_adapter:
            logger.error("Message adapter is not initialized. Cannot handle RPC request.")
            return None

        rpc_request: Optional[GatewayRPCRequest] = None
        result = None
        device_session: Optional[DeviceSession] = None
        try:
            data = self._message_adapter.deserialize_to_dict(payload)
            rpc_request = self._message_adapter.parse_rpc_request(topic, data)
            device_session = self._device_manager.get_by_name(rpc_request.device_name)
            if not device_session:
                logger.warning("No device session found for device: %s", rpc_request.device_name)
                return None
            logger.debug("Handling RPC method id: %i - %s with params: %s",
                         rpc_request.request_id, rpc_request.method, rpc_request.params)
            result = await self._event_dispatcher.dispatch(rpc_request, device_session=device_session)  # noqa
            if not result:
                return None
            elif not isinstance(result, GatewayRPCResponse):
                raise TypeError("RPC callback must return an instance of GatewayRPCResponse, got: %s", type(result))
            logger.debug("RPC response for device %r method id: %i - %s with result: %s",
                            rpc_request.device_name, result.request_id, rpc_request.method, result.result)
        except Exception as e:
            logger.exception("Failed to process RPC request: %s", e)
            if rpc_request is None:
                return None
            result = GatewayRPCResponse.build(device_name=rpc_request.device_name, request_id=rpc_request.request_id, error=e)

        if not device_session:
            logger.warning("No device session found for device: %s, cannot send RPC response",
                           rpc_request.device_name)
            return None

        future = await self._event_dispatcher.dispatch(result)  # noqa

        if not future:
            logger.warning("No publish futures were returned from message queue for RPC response of device %s, request id %i",
                           rpc_request.device_name, rpc_request.request_id)
            return None
        try:
            await await_or_stop(future, timeout=1, stop_event=self._stop_event)
        except TimeoutError:
            logger.warning("RPC response publish timed out for device %s, request id %i",
                           rpc_request.device_name, rpc_request.request_id)
