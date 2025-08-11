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
from collections import deque


class AsyncDeque:
    def __init__(self, maxlen):
        self._deque = deque(maxlen=maxlen)
        self._cond = asyncio.Condition()
        self._in_queue = set()

    async def put(self, item):
        if item.uuid in self._in_queue:
            return
        async with self._cond:
            self._deque.append(item)
            self._in_queue.add(item.uuid)
            self._cond.notify()

    async def extend(self, items):
        async with self._cond:
            for item in items:
                if item.uuid not in self._in_queue:
                    self._deque.append(item)
                    self._in_queue.add(item.uuid)
            self._cond.notify()

    async def put_left(self, item):
        if item.uuid in self._in_queue:
            return
        async with self._cond:
            self._deque.appendleft(item)
            self._in_queue.add(item.uuid)
            self._cond.notify()

    async def extend_left(self, items):
        async with self._cond:
            for item in reversed(items):
                if item.uuid not in self._in_queue:
                    self._deque.appendleft(item)
                    self._in_queue.add(item.uuid)
            self._cond.notify()

    async def get(self):
        async with self._cond:
            while not self._deque:
                await self._cond.wait()
            item = self._deque.popleft()
            self._in_queue.discard(item.uuid)
            return item

    async def peek(self):
        async with self._cond:
            while not self._deque:
                await self._cond.wait()
            return self._deque[0]

    async def peek_batch(self, max_count: int):
        async with self._cond:
            while not self._deque:
                await self._cond.wait()
            return list(self._deque)[:max_count]

    async def pop_n(self, n: int):
        async with self._cond:
            items = []
            for _ in range(min(n, len(self._deque))):
                item = self._deque.popleft()
                self._in_queue.discard(item.uuid)
                items.append(item)
            return items

    async def reinsert_front(self, item):
        async with self._cond:
            if item.uuid not in self._in_queue:
                self._deque.appendleft(item)
                self._in_queue.add(item.uuid)
            self._cond.notify()

    def is_empty(self):
        return not self._deque

    def size(self):
        return len(self._deque)
