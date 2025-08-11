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
from typing import Dict, Union, Awaitable, Callable, Optional, Tuple
from uuid import uuid4

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.device.message_adapter import MessageAdapter, JsonMessageAdapter

logger = get_logger(__name__)


class RPCResponseHandler:
    """
    Handles RPC responses coming from the platform to the client (client-side RPCs responses).
    Maintains an internal map of request_id -> asyncio.Future for awaiting RPC results.
    """

    def __init__(self):
        self._message_adapter: Optional[MessageAdapter] = None
        self._pending_rpc_requests: Dict[Union[str, int],
                                         Tuple[asyncio.Future[RPCResponse],
                                               Optional[Callable[[RPCResponse], Awaitable[None]]]]] = {}

    def set_message_adapter(self, message_adapter: MessageAdapter):
        """
        Sets the message adapter for handling incoming messages.
        This should be called before any requests are registered.
        :param message_adapter: An instance of MessageAdapter.
        """
        if not isinstance(message_adapter, MessageAdapter):
            raise ValueError("message_adapter must be an instance of MessageAdapter.")
        self._message_adapter = message_adapter
        logger.debug("Message adapter set for RPCResponseHandler.")

    def register_request(self, request_id: Union[str, int],
                         callback: Optional[Callable[[RPCResponse],
                                                     Awaitable[None]]] = None) -> asyncio.Future[RPCResponse]:
        """
        Called when a request is sent to the platform and a response is awaited.
        """
        if request_id in self._pending_rpc_requests:
            raise RuntimeError(f"Request ID {request_id} is already registered.")
        future = asyncio.get_event_loop().create_future()
        future.uuid = uuid4()
        self._pending_rpc_requests[request_id] = future, callback
        return future

    async def handle(self, topic: str, payload: Union[bytes, TimeoutError]):
        """
        Handles the incoming RPC response from the platform and fulfills the corresponding future.
        The topic is expected to be: v1/devices/me/rpc/response/{request_id}
        """
        try:
            if not self._message_adapter:
                dummy_adapter = JsonMessageAdapter()
                rpc_response = dummy_adapter.parse_rpc_response(topic, payload)
            else:
                rpc_response = self._message_adapter.parse_rpc_response(topic, payload)

            request_details = self._pending_rpc_requests.pop(rpc_response.request_id, None)
            if not request_details:
                logger.warning("No pending request found for request ID %s. Ignoring response.",
                               rpc_response.request_id)
                return
            future, callback = request_details
            if not future:
                logger.warning("No future awaiting request ID %s. Ignoring.", rpc_response.request_id)
                return
            if callback:
                try:
                    await callback(rpc_response)
                except Exception as e:
                    logger.exception("Error in callback for request ID %s: %s", rpc_response.request_id, e)
                    future.set_exception(e)
                    return

            if rpc_response.error:
                future.set_exception(Exception(rpc_response.error))
            else:
                future.set_result(rpc_response)

        except Exception as e:
            logger.exception("Failed to handle RPC response: %s", e)

    def clear(self):
        """
        Clears all pending futures (e.g., on disconnect).
        """
        for fut, _ in self._pending_rpc_requests.values():
            if not fut.done():
                fut.cancel()
        self._pending_rpc_requests.clear()
