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
from typing import Dict, Union

from orjson import loads

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.service.message_dispatcher import MessageDispatcher

logger = get_logger(__name__)


class RPCResponseHandler:
    """
    Handles RPC responses coming from the platform to the client (client-side RPCs responses).
    Maintains an internal map of request_id -> asyncio.Future for awaiting RPC results.
    """

    def __init__(self):
        self._message_dispatcher = None
        self._pending_rpc_requests: Dict[Union[str, int], asyncio.Future] = {}

    def set_message_dispatcher(self, message_dispatcher: MessageDispatcher):
        """
        Sets the message dispatcher for handling incoming messages.
        This should be called before any requests are registered.
        :param message_dispatcher: An instance of MessageDispatcher.
        """
        if not isinstance(message_dispatcher, MessageDispatcher):
            raise ValueError("message_dispatcher must be an instance of MessageDispatcher.")
        self._message_dispatcher = message_dispatcher
        logger.debug("Message dispatcher set for RPCResponseHandler.")

    def register_request(self, request_id: Union[str, int]) -> asyncio.Future:
        """
        Called when a request is sent to the platform and a response is awaited.
        """
        if request_id in self._pending_rpc_requests:
            raise RuntimeError(f"Request ID {request_id} is already registered.")
        future = asyncio.get_event_loop().create_future()
        self._pending_rpc_requests[request_id] = future
        return future

    async def handle(self, topic: str, payload: bytes):
        """
        Handles the incoming RPC response from the platform and fulfills the corresponding future.
        The topic is expected to be: v1/devices/me/rpc/response/{request_id}
        """
        try:
            # TODO: Use MessageDispatcher to parse the topic and payload
            request_id = topic.split("/")[-1]
            response_data = loads(payload)

            future = self._pending_rpc_requests.pop(request_id, None)
            if not future:
                logger.warning("No future awaiting request ID %s. Ignoring.", request_id)
                return

            if isinstance(response_data, dict) and "error" in response_data:
                future.set_exception(Exception(response_data["error"]))
            else:
                future.set_result(response_data)

        except Exception as e:
            logger.exception("Failed to handle RPC response: %s", e)

    def clear(self):
        """
        Clears all pending futures (e.g., on disconnect).
        """
        for fut in self._pending_rpc_requests.values():
            if not fut.done():
                fut.cancel()
        self._pending_rpc_requests.clear()
