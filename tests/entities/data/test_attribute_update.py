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

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate


@pytest.fixture
def example_entries():
    return [
        AttributeEntry("temperature", 23),
        AttributeEntry("humidity", 50),
        AttributeEntry("status", "ok")
    ]


def test_repr(example_entries):
    update = AttributeUpdate(example_entries)
    assert repr(update) == f"AttributeUpdate(entries={example_entries})"


def test_get_existing_key(example_entries):
    update = AttributeUpdate(example_entries)
    assert update.get("temperature") == 23
    assert update.get("status") == "ok"


def test_get_missing_key_with_default(example_entries):
    update = AttributeUpdate(example_entries)
    assert update.get("nonexistent") is None
    assert update.get("nonexistent", "default") == "default"


def test_keys(example_entries):
    update = AttributeUpdate(example_entries)
    assert update.keys() == ["temperature", "humidity", "status"]


def test_values(example_entries):
    update = AttributeUpdate(example_entries)
    assert update.values() == [23, 50, "ok"]


def test_items(example_entries):
    update = AttributeUpdate(example_entries)
    assert update.items() == [
        ("temperature", 23),
        ("humidity", 50),
        ("status", "ok")
    ]


def test_as_dict(example_entries):
    update = AttributeUpdate(example_entries)
    assert update.as_dict() == {
        "temperature": 23,
        "humidity": 50,
        "status": "ok"
    }


def test_deserialize_from_dict():
    raw = {
        "speed": 100,
        "enabled": True
    }
    update = AttributeUpdate._deserialize_from_dict(raw)
    assert isinstance(update, AttributeUpdate)
    assert update.as_dict() == raw
    assert update.get("speed") == 100
    assert update.get("enabled") is True
