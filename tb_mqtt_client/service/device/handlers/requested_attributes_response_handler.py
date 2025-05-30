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

from typing import Dict, Tuple, Awaitable, Callable

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.request_id_generator import AttributeRequestIdProducer
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.service.message_dispatcher import MessageDispatcher

logger = get_logger(__name__)


class RequestedAttributeResponseHandler:
    """
    Handles responses to attribute requests sent to the platform.
    """

    def __init__(self):
        self._message_dispatcher = None
        self._pending_attribute_requests: Dict[int, Tuple[AttributeRequest, Callable[[RequestedAttributeResponse], Awaitable[None]]]] = {}

    def set_message_dispatcher(self, message_dispatcher: MessageDispatcher):
        """
        Sets the message dispatcher for handling incoming messages.
        This should be called before any requests are registered.
        """
        if not isinstance(message_dispatcher, MessageDispatcher):
            raise ValueError("message_dispatcher must be an instance of MessageDispatcher.")
        self._message_dispatcher = message_dispatcher
        logger.debug("Message dispatcher set for RequestedAttributeResponseHandler.")

    async def register_request(self, request: AttributeRequest, callback: Callable[[RequestedAttributeResponse], Awaitable[None]]):
        """
        Called when a request is sent to the platform and a response is awaited.
        """
        request_id = request.request_id
        if request_id in self._pending_attribute_requests:
            raise RuntimeError(f"Request ID {request.request_id} is already registered.")
        self._pending_attribute_requests[request.request_id] = (request, callback)

    def unregister_request(self, request_id: int):
        """
        Unregisters a request by its ID.
        This is useful if the request is no longer needed or has timed out.
        """
        if request_id in self._pending_attribute_requests:
            self._pending_attribute_requests.pop(request_id)
            logger.debug("Unregistered attribute request with ID %s", request_id)
        else:
            logger.debug("Attempted to unregister non-existent request ID %s", request_id)

    async def handle(self, topic: str, payload: bytes):
        """
        Handles the incoming attribute request response.
        """
        try:
            if not self._message_dispatcher:
                logger.error("Message dispatcher is not initialized. Cannot handle attribute response.")
                request_id = topic.split('/')[-1]  # Assuming request ID is in the topic
                self._pending_attribute_requests.pop(int(request_id), (None, None, None))
                return

            requested_attribute_response = self._message_dispatcher.parse_attribute_request_response(topic, payload)
            pending_request_details = self._pending_attribute_requests.pop(requested_attribute_response.request_id, None)
            if not pending_request_details:
                logger.warning("No future awaiting request ID %s. Ignoring.", requested_attribute_response.request_id)
                return

            request, callback = pending_request_details

            if callback:
                logger.debug("Invoking callback for requested attribute response with ID %s", requested_attribute_response.request_id)
                await callback(requested_attribute_response)
            else:
                logger.error("No callback registered for requested attribute response with ID %s", requested_attribute_response.request_id)

        except Exception as e:
            logger.exception("Failed to handle requested attribute response: %s", e)

    def clear(self):
        """
        Clears all pending requests (e.g., on disconnect).
        """
        self._pending_attribute_requests.clear()
