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


@pytest.mark.parametrize("key, value", [
    ("temperature", 25),
    ("status", True),
    ("label", "sensor-A"),
    ("list_val", [1, 2, 3]),
    ("dict_val", {"k": "v"})
])
def test_attribute_entry_as_dict(key, value):
    entry = AttributeEntry(key, value)
    expected = {"key": key, "value": value}
    assert entry.as_dict() == expected


@pytest.mark.parametrize("key, value", [
    ("a", 1),
    ("b", "val"),
    ("c", {"k": "v"})
])
def test_attribute_entry_repr(key, value):
    entry = AttributeEntry(key, value)
    assert repr(entry) == f"AttributeEntry(key={key}, value={value})"


def test_attribute_entry_eq_same():
    e1 = AttributeEntry("k1", 123)
    e2 = AttributeEntry("k1", 123)
    assert e1 == e2


def test_attribute_entry_eq_diff_key():
    e1 = AttributeEntry("k1", 123)
    e2 = AttributeEntry("k2", 123)
    assert e1 != e2


def test_attribute_entry_eq_diff_value():
    e1 = AttributeEntry("k1", 123)
    e2 = AttributeEntry("k1", 456)
    assert e1 != e2


def test_attribute_entry_eq_wrong_type():
    e1 = AttributeEntry("k1", 123)
    assert e1 != {"key": "k1", "value": 123}
