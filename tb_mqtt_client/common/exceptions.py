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
import logging
from typing import Callable, Dict, List, Optional, Type

logger = logging.getLogger(__name__)

ExceptionCallback = Callable[[BaseException, Optional[dict]], None]


class ExceptionHandler:
    def __init__(self):
        self._callbacks: Dict[Type[BaseException], List[ExceptionCallback]] = {}
        self._default_callbacks: List[ExceptionCallback] = []

    def register(self, exc_type: Optional[Type[BaseException]], callback: ExceptionCallback):
        """
        Register a callback for a specific exception type or all (if exc_type is None).
        """
        if exc_type is None:
            self._default_callbacks.append(callback)
        else:
            self._callbacks.setdefault(exc_type, []).append(callback)

    def handle(self, exc: BaseException, context: Optional[dict] = None):
        """
        Dispatch the exception to the appropriate registered callbacks.
        """
        handled = False
        for exc_type, callbacks in self._callbacks.items():
            if isinstance(exc, exc_type):
                for cb in callbacks:
                    cb(exc, context)
                handled = True
        if not handled:
            for cb in self._default_callbacks:
                cb(exc, context)

    def install_asyncio_handler(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Hook into asyncio event loop to catch global task errors.
        """
        loop = loop or asyncio.get_event_loop()

        def _asyncio_handler(loop_, context: dict):
            exception = context.get("exception")
            if exception:
                self.handle(exception, context)
            else:
                logger.error("Unhandled asyncio context: %s", context)

        loop.set_exception_handler(_asyncio_handler)


class BackpressureException(Exception):
    """
    Exception raised when the client is under backpressure.
    This should be used to signal that the client cannot process more messages at the moment.
    """
    def __init__(self, message: str = "Client is under backpressure. Please retry later."):
        super().__init__(message)


exception_handler = ExceptionHandler()
