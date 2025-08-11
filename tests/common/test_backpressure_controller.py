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
from datetime import datetime, timedelta, UTC

import pytest

from tb_mqtt_client.common.rate_limit.backpressure_controller import BackpressureController


@pytest.fixture
def stop_event():
    return asyncio.Event()


@pytest.fixture
def controller(stop_event):
    return BackpressureController(stop_event)


def test_notify_quota_exceeded_initial(controller):
    controller.notify_quota_exceeded()
    assert controller._pause_until is not None
    assert controller._consecutive_quota_exceeded == 1


def test_notify_quota_exceeded_consecutive(controller):
    controller._last_quota_exceeded = datetime.now(UTC)
    controller._consecutive_quota_exceeded = 2
    controller.notify_quota_exceeded()
    assert controller._consecutive_quota_exceeded == 3
    assert controller._pause_until is not None


def test_notify_quota_exceeded_reset_after_60s(controller):
    controller._last_quota_exceeded = datetime.now(UTC) - timedelta(seconds=61)
    controller._consecutive_quota_exceeded = 5
    controller.notify_quota_exceeded()
    assert controller._consecutive_quota_exceeded == 1


def test_notify_quota_exceeded_custom_delay(controller):
    controller.notify_quota_exceeded(delay_seconds=30)
    assert controller._pause_until is not None


def test_notify_quota_exceeded_stop_event_set(stop_event, controller):
    stop_event.set()
    controller.notify_quota_exceeded()
    assert controller._pause_until is None


def test_notify_disconnect_default(controller):
    controller.notify_disconnect()
    assert controller._pause_until is not None


def test_notify_disconnect_custom_delay(controller):
    controller.notify_disconnect(delay_seconds=25)
    assert controller._pause_until is not None


def test_notify_disconnect_stop_event_set(stop_event, controller):
    stop_event.set()
    controller.notify_disconnect()
    assert controller._pause_until is None


def test_should_pause_active(controller):
    controller._pause_until = datetime.now(UTC) + timedelta(seconds=15)
    assert controller.should_pause()


def test_should_pause_expired(controller):
    controller._pause_until = datetime.now(UTC) - timedelta(seconds=1)
    assert controller.should_pause() is False
    assert controller._pause_until is None


def test_should_pause_not_set(controller):
    controller._pause_until = None
    assert controller.should_pause() is False


def test_should_pause_stop_event_set(stop_event, controller):
    stop_event.set()
    controller._pause_until = datetime.now(UTC) + timedelta(seconds=30)
    assert controller.should_pause() is False


def test_clear(controller):
    controller._pause_until = datetime.now(UTC) + timedelta(seconds=30)
    controller._consecutive_quota_exceeded = 5
    controller.clear()
    assert controller._pause_until is None
    assert controller._consecutive_quota_exceeded == 0
