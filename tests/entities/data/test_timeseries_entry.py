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
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry


@pytest.mark.parametrize(
    "key,value,ts,expected_dict",
    [
        ("temperature", 23.5, 1650000000000, {"key": "temperature", "value": 23.5, "ts": 1650000000000}),
        ("status", "online", None, {"key": "status", "value": "online"}),
        ("flag", True, 0, {"key": "flag", "value": True, "ts": 0}),
    ]
)
def test_as_dict(key, value, ts, expected_dict):
    entry = TimeseriesEntry(key, value, ts)
    assert entry.as_dict() == expected_dict


def test_repr_output():
    entry = TimeseriesEntry("humidity", 60, 1650001234567)
    assert repr(entry) == "TimeseriesEntry(key=humidity, value=60, ts=1650001234567)"


def test_repr_without_ts():
    entry = TimeseriesEntry("humidity", 60)
    assert repr(entry) == "TimeseriesEntry(key=humidity, value=60, ts=None)"


def test_equality_same_values():
    e1 = TimeseriesEntry("pressure", 101.3, 1650000000000)
    e2 = TimeseriesEntry("pressure", 101.3, 1650000000000)
    assert e1 == e2


def test_equality_different_key():
    e1 = TimeseriesEntry("a", 1, 123)
    e2 = TimeseriesEntry("b", 1, 123)
    assert e1 != e2


def test_equality_different_value():
    e1 = TimeseriesEntry("key", 1, 123)
    e2 = TimeseriesEntry("key", 2, 123)
    assert e1 != e2


def test_equality_different_ts():
    e1 = TimeseriesEntry("key", "value", 123)
    e2 = TimeseriesEntry("key", "value", 456)
    assert e1 != e2


def test_equality_different_type():
    e1 = TimeseriesEntry("key", "value", 123)
    assert e1 != {"key": "key", "value": "value", "ts": 123}


def test_default_ts_none():
    entry = TimeseriesEntry("key", "value")
    assert entry.ts is None
    assert entry.as_dict() == {"key": "key", "value": "value"}
