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
from collections import defaultdict
from typing import Callable, Awaitable, Dict, List, Union

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_event import GatewayEvent

EventCallback = Union[Callable[..., Awaitable[None]], Callable[..., None]]

logger = get_logger(__name__)


class EventDispatcher:
    """
    Direct event dispatcher for handling gateway events.
    """
    def __init__(self):
        self._handlers: Dict[GatewayEventType, List[EventCallback]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def register(self, event_type: GatewayEventType, callback: EventCallback):
        if callback not in self._handlers[event_type]:
            self._handlers[event_type].append(callback)

    def unregister(self, event_type: GatewayEventType, callback: EventCallback):
        if callback in self._handlers[event_type]:
            self._handlers[event_type].remove(callback)
            if not self._handlers[event_type]:
                del self._handlers[event_type]

    async def dispatch(self, event: GatewayEvent, *args, **kwargs):
        async with self._lock:
            callbacks = list(self._handlers.get(event.event_type, []))
        for cb in callbacks:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(event, *args, **kwargs)
                else:
                    cb(event, *args, **kwargs)
            except Exception as e:
                logger.error(f"[EventDispatcher] Exception in handler for '{event.event_type}': {e}")
