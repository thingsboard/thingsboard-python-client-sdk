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


from orjson import loads
from typing import Awaitable, Callable, Dict, Optional

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse

logger = get_logger(__name__)


class RPCRequestsHandler:
    """
    Handles incoming RPC request messages for a device.
    """

    def __init__(self):
        self._callback: Optional[Callable[[str, Dict], Awaitable[Dict]]] = None

    def set_callback(self, callback: Callable[[str, Dict], Awaitable[Dict]]):
        """
        Set the async callback to handle incoming RPC requests.
        :param callback: A coroutine accepting (method_name, params) and returning a result dict.
        """
        self._callback = callback

    async def handle(self, topic: str, payload: bytes) -> Optional[RPCResponse]:
        """
        Process the RPC request and return response payload and request ID (if possible).
        :returns: (request_id, response_dict) or None if failed
        """
        if not self._callback:
            logger.debug("No RPC request callback set. Skipping RPC handling. "
                         "You can add set callback using client.set_rpc_request_callback(your_method)")
            return None

        try:
            request_id = int(topic.split("/")[-1])
            parsed = loads(payload)
            parsed["id"] = request_id
            rpc_request = RPCRequest.from_dict(parsed)

            logger.debug("Handling RPC method id: %i - %s with params: %s",
                         rpc_request.request_id, rpc_request.method, rpc_request.params)

            result = await self._callback(rpc_request)
            if not isinstance(result, RPCResponse):
                logger.error("RPC callback must return an instance of RPCResponse, got: %s", type(result))
                return None
            return result

        except Exception as e:
            logger.exception("Failed to process RPC request: %s", e)
            return None
