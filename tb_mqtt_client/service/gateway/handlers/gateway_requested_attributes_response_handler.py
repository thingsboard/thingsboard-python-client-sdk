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
from asyncio import Task
from typing import Dict, Tuple, Coroutine, Callable, Union, TypeAlias, Any

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse
from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter

logger = get_logger(__name__)


AttributeResponseCallback: TypeAlias = Callable[[GatewayRequestedAttributeResponse], Coroutine[Any, Any, None]]


class GatewayRequestedAttributeResponseHandler:
    """
    Handles responses to attribute requests sent to the platform.
    """

    def __init__(self, event_dispatcher: Any, message_adapter: GatewayMessageAdapter, device_manager: DeviceManager):
        self._event_dispatcher = event_dispatcher
        self._message_adapter: Union[GatewayMessageAdapter, None] = message_adapter
        self._device_manager = device_manager
        self._pending_attribute_requests: Dict[Tuple[str, int], Tuple[GatewayAttributeRequest,
                                                                      Union[Task, None]]] = {}

    async def register_request(self,
                               request: GatewayAttributeRequest,
                               timeout: int = 30):
        """
        Called when a request is sent to the platform and a response is awaited.
        """
        request_id = request.request_id
        device_name = request.device_name
        key = (device_name, request_id)
        if key in self._pending_attribute_requests:
            raise RuntimeError(f"Request ID {request.request_id} is already registered.")
        timeout_task = None
        if timeout > 0:
            timeout_task = asyncio.get_event_loop().call_later(timeout, self._on_timeout, device_name, request_id)
        self._pending_attribute_requests[key] = (request, timeout_task)
        logger.debug("Registered attribute request with ID %s for device %s", request_id, device_name)

    def unregister_request(self, device_name: str, request_id: int):
        """
        Unregisters a request for device attributes by device name and request ID.
        This is useful if the request is no longer needed or has timed out.
        """
        key = (device_name, request_id)
        if key in self._pending_attribute_requests:
            self._pending_attribute_requests.pop(key)
            logger.debug("Unregistered attribute request with ID %s for device %s", request_id, device_name)
        else:
            logger.debug("Attempted to unregister non-existent request ID %s for device %s", request_id, device_name)

    async def handle(self, topic: str, payload: bytes):
        """
        Handles the incoming attribute request response.
        """
        try:
            if not self._message_adapter:
                logger.error("Message adapter is not initialized. Cannot handle requested attribute response.")
                return
            deserialized_data = self._message_adapter.deserialize_to_dict(payload)
            request_id = deserialized_data.get('id')
            device_name = deserialized_data.get('device')
            if request_id is None or device_name is None:
                logger.error("Received requested attribute response without 'id' or 'device'. ")
                return
            attribute_request_with_callback = self._pending_attribute_requests.get((device_name, request_id))
            if not attribute_request_with_callback:
                logger.warning("No pending request found for request ID %s. Ignoring response.", request_id)
                return
            attribute_request, timeout_task = attribute_request_with_callback
            if timeout_task:
                timeout_task.cancel()
            requested_attribute_response = self._message_adapter.parse_gateway_requested_attribute_response(attribute_request, deserialized_data)
            device_session = self._device_manager.get_by_name(device_name)
            if not device_session:
                logger.warning("No device session found for device: %s", device_name)
                return
            dispatch_task = self._event_dispatcher.dispatch(requested_attribute_response, device_session=device_session)

            logger.trace("Dispatching callback for requested attribute response with ID %s",
                         requested_attribute_response.request_id)
            task = asyncio.create_task(dispatch_task)
            task.add_done_callback(self._handle_callback_exception)

        except Exception as e:
            logger.exception("Failed to handle requested attribute response: %s", e)

    def _on_timeout(self, device_name: str, request_id: int):
        """
        Called when a request times out.
        Unregisters the request and logs a warning.
        """
        key = (device_name, request_id)
        if key in self._pending_attribute_requests:
            self._pending_attribute_requests.pop(key)
            logger.warning("Request ID %s for device %s has timed out and has been unregistered.", request_id, device_name)
        else:
            logger.debug("Attempted to unregister non-existent request ID %s for device %s on timeout", request_id, device_name)

    def _handle_callback_exception(self, task: asyncio.Task):
        try:
            task.result()
        except Exception as e:
            logger.exception("Exception in user-defined requested attribute callback: %s", e)

    def clear(self):
        """
        Clears all pending requests (e.g., on disconnect).
        """
        self._pending_attribute_requests.clear()
