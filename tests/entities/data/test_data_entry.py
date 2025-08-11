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

from unittest.mock import patch

import pytest

from tb_mqtt_client.entities.data.data_entry import DataEntry


def test_data_entry_initialization_without_timestamp():
    entry = DataEntry("temperature", 22.5)

    assert entry.key == "temperature"
    assert entry.value == 22.5
    assert entry.ts is None
    assert isinstance(entry.size, int)
    assert "DataEntry(key=temperature, value=22.5, ts=None)" == repr(entry)


def test_data_entry_initialization_with_timestamp():
    entry = DataEntry("humidity", 60, ts=1717171717)

    assert entry.key == "humidity"
    assert entry.value == 60
    assert entry.ts == 1717171717
    assert isinstance(entry.size, int)
    assert "DataEntry(key=humidity, value=60, ts=1717171717)" == repr(entry)


def test_data_entry_key_setter_recalculates_size():
    entry = DataEntry("initial", True)
    original_size = entry.size
    entry.key = "updated_key"

    assert entry.key == "updated_key"
    assert isinstance(entry.size, int)
    assert entry.size != original_size


def test_data_entry_value_setter_recalculates_size():
    entry = DataEntry("key", "initial")
    original_size = entry.size
    entry.value = "updated_value"

    assert entry.value == "updated_value"
    assert isinstance(entry.size, int)
    assert entry.size != original_size


def test_data_entry_ts_setter_recalculates_size():
    entry = DataEntry("voltage", 3.3)
    original_size = entry.size
    entry.ts = 1710000000

    assert entry.ts == 1710000000
    assert isinstance(entry.size, int)
    assert entry.size != original_size


def test_data_entry_raises_on_invalid_json():
    class NonSerializable:
        pass

    with pytest.raises(ValueError, match="unsupported - NonSerializable"):
        DataEntry("bad", NonSerializable())


@patch("tb_mqtt_client.entities.data.data_entry.dumps")
def test_estimate_size_called_with_ts(mock_dumps):
    mock_dumps.return_value = b'{"ts":123,"values":{"x":42}}'
    entry = DataEntry("x", 42, ts=123)

    mock_dumps.assert_called_once_with({"ts": 123, "values": {"x": 42}})
    assert entry.size == len(mock_dumps.return_value)


@patch("tb_mqtt_client.entities.data.data_entry.dumps")
def test_estimate_size_called_without_ts(mock_dumps):
    mock_dumps.return_value = b'{"x":42}'
    entry = DataEntry("x", 42)

    mock_dumps.assert_called_once_with({"x": 42})
    assert entry.size == len(mock_dumps.return_value)
