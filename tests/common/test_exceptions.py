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
from typing import List, Tuple, Optional

import pytest

from tb_mqtt_client.common.exceptions import ExceptionHandler, BackpressureException


def make_recorder(storage: List[Tuple[str, Optional[dict]]]):
    def _recorder(exc: BaseException, context: Optional[dict]):
        storage.append((exc.__class__.__name__, context))

    return _recorder


def test_register_and_handle_specific_exception_calls_callback():
    handler = ExceptionHandler()
    events: List[Tuple[str, Optional[dict]]] = []
    cb = make_recorder(events)

    handler.register(ValueError, cb)

    err = ValueError("boom")
    ctx = {"info": "ctx"}
    handler.handle(err, ctx)

    assert events == [("ValueError", ctx)]


def test_default_callback_called_when_no_specific_registered():
    handler = ExceptionHandler()
    events: List[Tuple[str, Optional[dict]]] = []
    default_cb = make_recorder(events)

    handler.register(None, default_cb)

    err = KeyError("missing")
    ctx = {"k": "v"}
    handler.handle(err, ctx)

    assert events == [("KeyError", ctx)]


def test_specific_takes_precedence_default_not_called():
    handler = ExceptionHandler()
    events: List[Tuple[str, Optional[dict]]] = []
    specific_events: List[Tuple[str, Optional[dict]]] = []

    default_cb = make_recorder(events)
    specific_cb = make_recorder(specific_events)

    handler.register(None, default_cb)
    handler.register(RuntimeError, specific_cb)

    err = RuntimeError("nope")
    ctx = {"tag": 1}
    handler.handle(err, ctx)

    assert specific_events == [("RuntimeError", ctx)]
    assert events == []


def test_multiple_callbacks_for_same_exception_type_all_called():
    handler = ExceptionHandler()
    calls: List[Tuple[str, Optional[dict]]] = []

    cb1 = make_recorder(calls)
    cb2 = make_recorder(calls)
    handler.register(IndexError, cb1)
    handler.register(IndexError, cb2)

    err = IndexError("oops")
    ctx = {"n": 42}
    handler.handle(err, ctx)

    assert calls == [("IndexError", ctx), ("IndexError", ctx)]


def test_subclass_matching_triggers_all_matching_callbacks_and_skips_default():
    handler = ExceptionHandler()

    calls_exception: List[Tuple[str, Optional[dict]]] = []
    calls_valueerror: List[Tuple[str, Optional[dict]]] = []
    default_calls: List[Tuple[str, Optional[dict]]] = []

    cb_exception = make_recorder(calls_exception)
    cb_valueerror = make_recorder(calls_valueerror)
    cb_default = make_recorder(default_calls)

    handler.register(Exception, cb_exception)
    handler.register(ValueError, cb_valueerror)
    handler.register(None, cb_default)

    err = ValueError("bad value")
    ctx = {"a": 1}
    handler.handle(err, ctx)

    assert len(calls_exception) == 1
    assert len(calls_valueerror) == 1
    assert calls_exception[0][0] == "ValueError"
    assert calls_valueerror[0][0] == "ValueError"

    assert default_calls == []


@pytest.mark.asyncio
async def test_install_asyncio_handler_dispatches_on_loop_exception(event_loop: asyncio.AbstractEventLoop):
    """
    Instead of relying on an un-awaited task (flaky under uvloop),
    directly call the loop's exception handler with a context containing an exception.
    This validates that install_asyncio_handler wires _asyncio_handler correctly,
    and that it dispatches to ExceptionHandler.handle(...).
    """
    handler = ExceptionHandler()

    calls: List[Tuple[str, Optional[dict]]] = []
    cb = make_recorder(calls)
    handler.register(ValueError, cb)

    handler.install_asyncio_handler(event_loop)

    err = ValueError("async oops")
    context = {"exception": err, "message": "unit-test"}
    event_loop.call_exception_handler(context)

    await asyncio.sleep(0)

    assert calls == [("ValueError", context)]


def test_backpressure_exception_default_message():
    exc = BackpressureException()
    assert str(exc) == "Client is under backpressure. Please retry later."


def test_backpressure_exception_custom_message():
    exc = BackpressureException("Hold up!")
    assert str(exc) == "Hold up!"


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
