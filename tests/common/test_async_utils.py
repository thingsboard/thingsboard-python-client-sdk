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
import threading
import time

import pytest

import tb_mqtt_client.common.async_utils as async_utils_mod
from tb_mqtt_client.common.async_utils import FutureMap, await_or_stop, run_coroutine_sync
from tb_mqtt_client.common.publish_result import PublishResult


@pytest.fixture
def fake_loop(monkeypatch):
    class _FakeTask:
        def __init__(self, thread: threading.Thread):
            self._thread = thread

        def done(self):  # optional helpers if ever needed
            return not self._thread.is_alive()

    class _FakeLoop:
        def create_task(self, coro):
            t = threading.Thread(target=lambda: asyncio.run(coro), daemon=True)
            t.start()
            return _FakeTask(t)

    monkeypatch.setattr(async_utils_mod.asyncio, "get_running_loop", lambda: _FakeLoop())

    return _FakeLoop()


@pytest.mark.asyncio
async def test_future_map_register_and_get_parents():
    fm = FutureMap()
    parent = asyncio.Future()
    child1, child2 = asyncio.Future(), asyncio.Future()

    fm.register(parent, [child1])
    assert fm.get_parents(child1) == [parent]

    # Add more children to same parent
    fm.register(parent, [child2])
    assert set(fm.get_parents(child1)) == {parent}
    assert set(fm.get_parents(child2)) == {parent}


@pytest.mark.asyncio
async def test_future_map_child_resolved_merges_results():
    fm = FutureMap()
    parent = asyncio.Future()
    child1, child2 = asyncio.Future(), asyncio.Future()

    fm.register(parent, [child1, child2])
    child1.set_result(PublishResult("topic", 1, 1, 1, 0))
    child2.set_result(PublishResult("topic", 1, 1, 1, 0))

    # Resolving child1 won't complete parent
    fm.child_resolved(child1)
    assert not parent.done()

    # Resolving last child completes parent with merged PublishResult
    fm.child_resolved(child2)
    assert parent.done()
    result = parent.result()
    assert isinstance(result, PublishResult)
    assert result.reason_code == 0


@pytest.mark.asyncio
async def test_future_map_child_resolved_no_publish_result():
    fm = FutureMap()
    parent = asyncio.Future()
    child = asyncio.Future()

    fm.register(parent, [child])
    child.set_result("not publish result")
    fm.child_resolved(child)
    assert parent.done()
    assert parent.result() is None


@pytest.mark.asyncio
async def test_future_map_child_resolved_with_cancelled_child():
    fm = FutureMap()
    parent = asyncio.Future()
    child = asyncio.Future()
    fm.register(parent, [child])
    child.cancel()
    fm.child_resolved(child)
    assert parent.done()
    assert parent.result() is None


@pytest.mark.asyncio
async def test_await_or_stop_coroutine_finishes_first():
    stop_event = asyncio.Event()

    async def coro(): return 123

    result = await await_or_stop(coro(), stop_event, timeout=1)
    assert result == 123


@pytest.mark.asyncio
async def test_await_or_stop_stop_event_first():
    stop_event = asyncio.Event()

    async def coro(): await asyncio.sleep(0.5)

    asyncio.get_event_loop().call_soon(stop_event.set)
    result = await await_or_stop(coro(), stop_event, timeout=1)
    assert result is None


@pytest.mark.asyncio
async def test_await_or_stop_timeout():
    stop_event = asyncio.Event()

    async def coro(): await asyncio.sleep(1)

    with pytest.raises(asyncio.TimeoutError):
        await await_or_stop(coro(), stop_event, timeout=0.01)


@pytest.mark.asyncio
async def test_await_or_stop_negative_timeout():
    stop_event = asyncio.Event()

    async def coro(): return "ok"

    result = await await_or_stop(coro(), stop_event, timeout=-1)
    assert result == "ok"


@pytest.mark.asyncio
async def test_await_or_stop_future_done():
    stop_event = asyncio.Event()
    fut = asyncio.Future()
    fut.set_result("done")
    result = await await_or_stop(fut, stop_event, timeout=1)
    assert result == "done"


@pytest.mark.asyncio
async def test_await_or_stop_invalid_type():
    stop_event = asyncio.Event()
    with pytest.raises(TypeError):
        await await_or_stop("not a future", stop_event, timeout=1)


@pytest.mark.asyncio
async def test_await_or_stop_cancelled_error():
    stop_event = asyncio.Event()

    async def coro():
        raise asyncio.CancelledError()

    result = await await_or_stop(coro(), stop_event, timeout=1)
    assert result is None


def test_returns_result_when_coroutine_completes(fake_loop):
    async def ok_coro():
        await asyncio.sleep(0.01)
        return "done"

    result = run_coroutine_sync(lambda: ok_coro(), timeout=0.5)
    assert result == "done"


def test_raises_original_exception_from_coroutine(fake_loop):
    class CustomError(RuntimeError):
        pass

    async def bad_coro():
        await asyncio.sleep(0.01)
        raise CustomError("boom")

    with pytest.raises(CustomError, match="boom"):
        run_coroutine_sync(lambda: bad_coro(), timeout=0.5)


def test_timeout_raises_timeout_error(fake_loop):
    async def slow_coro():
        await asyncio.sleep(0.2)
        return "too late"

    with pytest.raises(TimeoutError, match=r"did not complete in 0\.05 seconds"):
        run_coroutine_sync(lambda: slow_coro(), timeout=0.05, raise_on_timeout=True)
    time.sleep(0.25)


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
