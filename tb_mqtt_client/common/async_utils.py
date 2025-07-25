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
import threading
from typing import Union, Optional, Any, List, Set, Dict
import asyncio

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.publish_result import PublishResult

logger = get_logger(__name__)


class FutureMap:
    def __init__(self):
        self._child_to_parents: Dict[asyncio.Future, Set[asyncio.Future]] = {}
        self._parent_to_remaining: Dict[asyncio.Future, Set[asyncio.Future]] = {}

    def register(self, parent: asyncio.Future, children: List[asyncio.Future]):
        if parent in self._parent_to_remaining:
            self._parent_to_remaining[parent].update(children)
        else:
            self._parent_to_remaining[parent] = set(children)

        for child in children:
            self._child_to_parents.setdefault(child, set()).add(parent)

    def get_parents(self, child: asyncio.Future) -> List[asyncio.Future]:
        return list(self._child_to_parents.get(child, []))

    def child_resolved(self, child: asyncio.Future):
        parents = self._child_to_parents.pop(child, set())
        for parent in parents:
            remaining = self._parent_to_remaining.get(parent)
            if remaining is not None:
                remaining.discard(child)
                if not remaining and not parent.done():
                    all_children = list(remaining) + [child]
                    results = []
                    for f in all_children:
                        if f.done() and not f.cancelled():
                            result = f.result()
                            if isinstance(result, PublishResult):
                                results.append(result)

                    if results:
                        parent.set_result(PublishResult.merge(results))
                    else:
                        parent.set_result(None)
                    self._parent_to_remaining.pop(parent, None)

future_map = FutureMap()

async def await_or_stop(future_or_coroutine: Union[asyncio.Future, asyncio.Task, Any],
                        stop_event: asyncio.Event,
                        timeout: Optional[float]) -> Optional[Any]:
    if asyncio.iscoroutine(future_or_coroutine):
        main_task = asyncio.create_task(future_or_coroutine)
    elif asyncio.isfuture(future_or_coroutine):
        if future_or_coroutine.done():
            return future_or_coroutine.result()
        main_task = future_or_coroutine
    else:
        raise TypeError("Expected coroutine or Future/Task")

    stop_task = asyncio.create_task(stop_event.wait())

    if timeout is not None and timeout < 0:
        timeout = None

    try:
        done, _ = await asyncio.wait(
            [main_task, stop_task],
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED
        )
        if main_task in done:
            return await main_task
        if stop_task in done:
            if stop_event.is_set():
                return None
        if timeout is not None and not done:
            raise asyncio.TimeoutError("Operation timed out")
    except asyncio.CancelledError:
        return None
    finally:
        if not stop_task.done():
            stop_task.cancel()

async def await_and_resolve_original(
    parent_futures: List[asyncio.Future],
    child_futures: List[asyncio.Future]
):
    try:
        results = await asyncio.gather(*child_futures, return_exceptions=True)

        for child in child_futures:
            future_map.child_resolved(child)

        for i, f in enumerate(parent_futures):
            if f is not None and not f.done():
                first_result = next((r for r in results if not isinstance(r, Exception)), None)
                first_exception = next((r for r in results if isinstance(r, Exception)), None)

                if first_exception and not first_result:
                    f.set_exception(first_exception)
                    logger.debug("Set exception for parent future #%d id=%r from child exception: %r",
                                 i, getattr(f, 'uuid', f), first_exception)
                else:
                    f.set_result(first_result)
                    logger.trace("Resolved parent future #%d id=%r with result: %r",
                                 i, getattr(f, 'uuid', f), first_result)

    except Exception as e:
        logger.error("Unexpected error while resolving parent delivery futures: %s", e)
        for i, f in enumerate(parent_futures):
            if f is not None and not f.done():
                f.set_exception(e)
                logger.debug("Set fallback exception for parent future #%d id=%r", i, getattr(f, 'uuid', f))


def run_coroutine_sync(coro_func, timeout: float = 3.0, raise_on_timeout: bool = False):
    """
    Run async coroutine and return its result from a sync function even if event loop is running.
    :param coro_func: async function with no arguments (like: lambda: some_async_fn())
    :param timeout: max wait time in seconds
    :param raise_on_timeout: if True, raise TimeoutError on timeout; otherwise return None
    """
    result_container = {}
    event = threading.Event()

    async def wrapper():
        try:
            result = await coro_func()
            result_container['result'] = result
        except Exception as e:
            result_container['error'] = e
        finally:
            event.set()

    loop = asyncio.get_running_loop()
    loop.create_task(wrapper())

    completed = event.wait(timeout=timeout)

    if not completed:
        logger.warning("Timeout while waiting for coroutine to finish: %s", coro_func)
        if raise_on_timeout:
            raise TimeoutError(f"Coroutine {coro_func} did not complete in {timeout} seconds.")
        return None

    if 'error' in result_container:
        raise result_container['error']

    return result_container.get('result')