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

import pytest
from tb_mqtt_client.common.publish_result import PublishResult


@pytest.fixture
def default_publish_result():
    return PublishResult(
        topic="v1/devices/me/telemetry",
        qos=1,
        message_id=123,
        payload_size=256,
        reason_code=0
    )


def test_publish_result_attributes(default_publish_result):
    assert default_publish_result.topic == "v1/devices/me/telemetry"
    assert default_publish_result.qos == 1
    assert default_publish_result.message_id == 123
    assert default_publish_result.payload_size == 256
    assert default_publish_result.reason_code == 0


def test_publish_result_repr(default_publish_result):
    result = repr(default_publish_result)
    assert isinstance(result, str)
    assert "PublishResult" in result
    assert "v1/devices/me/telemetry" in result
    assert "qos=1" in result
    assert "message_id=123" in result
    assert "payload_size=256" in result
    assert "reason_code=0" in result
    assert "datapoints_count=0" in result


def test_publish_result_as_dict(default_publish_result):
    d = default_publish_result.as_dict()
    assert isinstance(d, dict)
    assert d == {
        "topic": "v1/devices/me/telemetry",
        "qos": 1,
        "message_id": 123,
        "payload_size": 256,
        "reason_code": 0,
        "datapoints_count": 0
    }


def test_publish_result_is_successful_true(default_publish_result):
    assert default_publish_result.is_successful() is True


def test_publish_result_is_successful_false():
    result = PublishResult(
        topic="v1/devices/me/attributes",
        qos=0,
        message_id=999,
        payload_size=0,
        reason_code=128  # Simulated failure
    )
    assert result.is_successful() is False


@pytest.mark.parametrize("reason_code", [1, 2, 3, 16, 255])
def test_publish_result_various_failure_codes(reason_code):
    result = PublishResult(
        topic="v1/devices/me/rpc",
        qos=2,
        message_id=42,
        payload_size=100,
        reason_code=reason_code
    )
    assert result.is_successful() is False
