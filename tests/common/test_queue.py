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
from types import SimpleNamespace

import pytest

from tb_mqtt_client.common.queue import AsyncDeque


@pytest.fixture
def make_item():
    """Factory to create unique or duplicate-like test items."""

    def _make(uuid):
        return SimpleNamespace(uuid=uuid)

    return _make


@pytest.mark.asyncio
async def test_put_and_get(make_item):
    q = AsyncDeque(maxlen=5)
    item = make_item("a")
    await q.put(item)
    assert q.size() == 1
    assert not q.is_empty()

    got = await q.get()
    assert got.uuid == "a"
    assert q.is_empty()


@pytest.mark.asyncio
async def test_put_duplicate_not_added(make_item):
    q = AsyncDeque(maxlen=5)
    item = make_item("dup")
    await q.put(item)
    await q.put(item)  # duplicate
    assert q.size() == 1


@pytest.mark.asyncio
async def test_extend_and_duplicates(make_item):
    q = AsyncDeque(maxlen=5)
    items = [make_item(str(i)) for i in range(3)]
    await q.extend(items)
    assert q.size() == 3

    # Duplicate extend
    await q.extend(items)
    assert q.size() == 3


@pytest.mark.asyncio
async def test_put_left_and_duplicate(make_item):
    q = AsyncDeque(maxlen=5)
    item = make_item("x")
    await q.put_left(item)
    assert list(await q.peek_batch(1))[0].uuid == "x"

    await q.put_left(item)  # duplicate, ignored
    assert q.size() == 1


@pytest.mark.asyncio
async def test_extend_left_and_duplicates(make_item):
    q = AsyncDeque(maxlen=5)
    items = [make_item(str(i)) for i in range(3)]
    await q.extend_left(items)
    assert [it.uuid for it in await q.peek_batch(3)] == ["0", "1", "2"]

    await q.extend_left(items)  # duplicates ignored
    assert q.size() == 3


@pytest.mark.asyncio
async def test_peek_and_peek_batch_waits(make_item):
    q = AsyncDeque(maxlen=5)

    async def delayed_put():
        await asyncio.sleep(0.01)
        await q.put(make_item("peeked"))

    asyncio.create_task(delayed_put())
    first = await q.peek()
    assert first.uuid == "peeked"

    batch = await q.peek_batch(1)
    assert batch[0].uuid == "peeked"


@pytest.mark.asyncio
async def test_pop_n_partial_and_full(make_item):
    q = AsyncDeque(maxlen=5)
    items = [make_item(str(i)) for i in range(3)]
    await q.extend(items)

    # pop more than available
    popped = await q.pop_n(5)
    assert [it.uuid for it in popped] == ["0", "1", "2"]
    assert q.is_empty()

    # fill again
    await q.extend(items)
    popped2 = await q.pop_n(2)
    assert [it.uuid for it in popped2] == ["0", "1"]
    assert q.size() == 1


@pytest.mark.asyncio
async def test_reinsert_front_duplicate_and_new(make_item):
    q = AsyncDeque(maxlen=5)
    item1 = make_item("1")
    item2 = make_item("2")

    # Insert new
    await q.reinsert_front(item1)
    assert q.size() == 1
    assert (await q.peek()).uuid == "1"

    # Try duplicate
    await q.reinsert_front(item1)
    assert q.size() == 1

    # Insert another new
    await q.reinsert_front(item2)
    assert q.size() == 2
    assert (await q.peek()).uuid == "2"


@pytest.mark.asyncio
async def test_get_waits_until_item_available(make_item):
    q = AsyncDeque(maxlen=5)

    async def delayed_put():
        await asyncio.sleep(0.01)
        await q.put(make_item("later"))

    asyncio.create_task(delayed_put())
    got = await q.get()
    assert got.uuid == "later"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
