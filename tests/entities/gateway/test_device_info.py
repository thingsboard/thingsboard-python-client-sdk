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

import uuid

import pytest

from tb_mqtt_client.entities.gateway.device_info import DeviceInfo


def test_post_init_and_basic_properties():
    """Test that __post_init__ sets original_name and _initializing is False."""
    d = DeviceInfo(device_name="dev1", device_profile="profile1")
    assert d.device_name == "dev1"
    assert d.original_name == "dev1"
    assert isinstance(d.device_id, uuid.UUID)
    assert not d._initializing


def test_setattr_after_init_raises():
    """Test that modifying attributes after init raises AttributeError."""
    d = DeviceInfo(device_name="dev1", device_profile="p1")
    with pytest.raises(AttributeError) as exc:
        d.device_name = "new_name"
    assert "Cannot modify attribute" in str(exc.value)


def test_rename_changes_name():
    """Test rename method changes device_name."""
    d = DeviceInfo(device_name="dev1", device_profile="p1")
    old_id = d.device_id
    d.rename("new_dev")
    assert d.device_name == "new_dev"
    assert d.device_id == old_id  # ID stays the same


def test_rename_same_name_no_change():
    """Renaming to the same name should not alter anything."""
    d = DeviceInfo(device_name="dev1", device_profile="p1")
    d.rename("dev1")  # No change expected
    assert d.device_name == "dev1"


def test_from_dict_and_to_dict():
    """Test creating from dict and converting back to dict."""
    dev_id = str(uuid.uuid4())
    data = {
        "device_name": "dname",
        "device_profile": "prof",
        "device_id": dev_id,
        "original_name": "orig"
    }
    d = DeviceInfo.from_dict(data)
    assert isinstance(d, DeviceInfo)
    assert str(d.device_id) == dev_id
    assert d.device_name == "dname"
    assert d.original_name == "orig"

    # Check to_dict output
    out = d.to_dict()
    assert out["device_name"] == "dname"
    assert out["device_profile"] == "prof"
    assert out["device_id"] == dev_id
    assert out["original_name"] == "orig"


def test_str_and_repr():
    """Test __str__ and __repr__ produce expected formats."""
    d = DeviceInfo(device_name="dname", device_profile="prof")
    s = str(d)
    r = repr(d)
    assert "DeviceInfo" in s
    assert "DeviceInfo" in r
    assert str(d.device_id) in s
    assert repr(d.device_id) in r


def test_eq_and_hash():
    """Test equality and hash implementation."""
    d1 = DeviceInfo(device_name="dname", device_profile="prof")
    d2 = DeviceInfo(device_name="dname", device_profile="prof")
    # Manually make IDs equal for equality check
    object.__setattr__(d2, "device_id", d1.device_id)
    object.__setattr__(d2, "original_name", d1.original_name)

    assert d1 == d2
    assert hash(d1) == hash(d2)

    # Different object type should return NotImplemented for eq
    assert d1.__eq__("not a DeviceInfo") is NotImplemented


def test_hash_works_in_set():
    """Test that DeviceInfo is hashable and works in a set."""
    d = DeviceInfo(device_name="dname", device_profile="prof")
    s = {d}
    assert d in s

if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])