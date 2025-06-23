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

from typing import Union, Optional, Any
import asyncio


async def await_or_stop(future_or_coroutine: Union[asyncio.Future, asyncio.Task, Any],
                        stop_event: asyncio.Event,
                        timeout: Optional[float]) -> Optional[Any]:
    """
    Await the given future/coroutine until it completes, timeout expires, or stop_event is set.

    :param future_or_coroutine: An awaitable coroutine, asyncio.Future, or asyncio.Task.
    :param stop_event: asyncio.Event that signals shutdown.
    :param timeout: Optional timeout in seconds, -1 for no timeout, or None to wait indefinitely.
    :return: The result if completed successfully, or None on timeout/stop.
    """
    if asyncio.iscoroutine(future_or_coroutine):
        main_task = asyncio.create_task(future_or_coroutine)
    elif asyncio.isfuture(future_or_coroutine):
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
