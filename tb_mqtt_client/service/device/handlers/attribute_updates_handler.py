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

from typing import Awaitable, Callable, Optional

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.service.device.message_adapter import MessageAdapter

logger = get_logger(__name__)


class AttributeUpdatesHandler:
    """
    Handles shared attribute update messages from the platform.
    """

    def __init__(self):
        self._message_adapter = None
        self._callback: Optional[Callable[[AttributeUpdate], Awaitable[None]]] = None

    def set_message_adapter(self, message_adapter: MessageAdapter):
        """
        Sets the message adapter for handling incoming messages.
        This should be called before any callbacks are set.

        :param message_adapter: An instance of MessageAdapter.
        """
        if not isinstance(message_adapter, MessageAdapter):
            raise ValueError("message_adapter must be an instance of MessageAdapter.")
        self._message_adapter = message_adapter
        logger.debug("Message adapter set for AttributeUpdatesHandler.")

    def set_callback(self, callback: Callable[[AttributeUpdate], Awaitable[None]]):
        """
        Sets the async callback that will be triggered on shared attribute update.

        :param callback: A coroutine that takes an AttributeUpdate object.
        """
        self._callback = callback

    async def handle(self, topic: str, payload: bytes):  # noqa
        if not self._callback:
            logger.debug("No attribute update callback set. Skipping payload.")
            return

        try:
            data = self._message_adapter.parse_attribute_update(payload)
            logger.debug("Handling attribute update: %r", data)
            await self._callback(data)
        except Exception as e:
            logger.exception("Failed to handle attribute update: %s", e)
