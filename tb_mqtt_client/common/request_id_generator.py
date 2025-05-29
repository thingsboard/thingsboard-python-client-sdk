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

from asyncio import Lock


class RPCRequestIdProducer:
    """
    Singleton-style producer of unique RPC request IDs,
    safe for async environments and shared across all SDK services.
    """

    _counter: int = 1
    _lock: Lock = Lock()

    @classmethod
    async def get_next(cls) -> int:
        """
        Atomically increment and return the next request ID.
        """
        async with cls._lock:
            current = cls._counter
            cls._counter += 1
            return current

    @classmethod
    def reset(cls):
        """
        Reset the global request ID counter (usually on disconnect).
        """
        cls._counter = 1


class AttributeRequestIdProducer:
    """
    Singleton-style producer of unique attribute request IDs,
    safe for async environments and shared across all SDK services.
    """

    _counter: int = 1
    _lock: Lock = Lock()

    @classmethod
    async def get_next(cls) -> int:
        """
        Atomically increment and return the next attribute request ID.
        """
        async with cls._lock:
            current = cls._counter
            cls._counter += 1
            return current

    @classmethod
    def reset(cls):
        """
        Reset the global attribute request ID counter (usually on disconnect).
        """
        cls._counter = 1
