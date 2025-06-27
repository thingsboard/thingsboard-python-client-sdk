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
from collections import OrderedDict
from types import MappingProxyType

import pytest

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder, DeviceUplinkMessage, \
    DEFAULT_FIELDS_SIZE
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry


@pytest.fixture
def attribute_entry():
    return AttributeEntry(key="temp", value=42)


@pytest.fixture
def timeseries_entry():
    return TimeseriesEntry(key="speed", value=88, ts=1234567890)


def test_direct_instantiation_forbidden():
    with pytest.raises(TypeError, match="Direct instantiation of DeviceUplinkMessage is not allowed"):
        DeviceUplinkMessage(device_name="test", device_profile="default", attributes=(), timeseries={},
                            delivery_futures=[], _size=0)


def test_build_empty_message():
    builder = DeviceUplinkMessageBuilder()
    msg = builder.build()

    assert msg.device_name is None
    assert msg.device_profile is None
    assert msg.attributes == ()
    assert isinstance(msg.timeseries, MappingProxyType)
    assert dict(msg.timeseries) == {}
    assert len(msg.delivery_futures) == 1
    assert isinstance(msg.delivery_futures[0], asyncio.Future)
    assert msg.size == DEFAULT_FIELDS_SIZE
    assert not msg.has_attributes()
    assert not msg.has_timeseries()
    assert msg.attributes_datapoint_count() == 0
    assert msg.timeseries_datapoint_count() == 0


def test_set_device_name_and_profile():
    builder = DeviceUplinkMessageBuilder()
    msg = builder.set_device_name("device-1").set_device_profile("profile-x").build()

    assert msg.device_name == "device-1"
    assert msg.device_profile == "profile-x"
    assert msg.size == DEFAULT_FIELDS_SIZE + len("device-1") + len("profile-x")


def test_add_single_attribute(attribute_entry):
    builder = DeviceUplinkMessageBuilder()
    msg = builder.add_attributes(attribute_entry).build()

    assert len(msg.attributes) == 1
    assert msg.attributes[0] == attribute_entry
    assert msg.attributes_datapoint_count() == 1
    assert msg.has_attributes()
    assert msg.size == DEFAULT_FIELDS_SIZE + attribute_entry.size


def test_add_multiple_attributes(attribute_entry):
    other = AttributeEntry(key="humidity", value=60)
    builder = DeviceUplinkMessageBuilder()
    msg = builder.add_attributes([attribute_entry, other]).build()

    assert len(msg.attributes) == 2
    assert msg.attributes == (attribute_entry, other)
    expected_size = DEFAULT_FIELDS_SIZE + attribute_entry.size + other.size
    assert msg.size == expected_size


def test_add_single_timeseries_entry(timeseries_entry):
    builder = DeviceUplinkMessageBuilder()
    msg = builder.add_timeseries(timeseries_entry).build()

    assert len(msg.timeseries) == 1
    assert timeseries_entry.ts in msg.timeseries
    assert msg.timeseries[timeseries_entry.ts] == (timeseries_entry,)
    assert msg.has_timeseries()
    assert msg.timeseries_datapoint_count() == 1
    assert msg.size == DEFAULT_FIELDS_SIZE + timeseries_entry.size


def test_add_multiple_timeseries_entries(timeseries_entry):
    entry2 = TimeseriesEntry(key="vibration", value=1.5, ts=timeseries_entry.ts)
    builder = DeviceUplinkMessageBuilder()
    msg = builder.add_timeseries([timeseries_entry, entry2]).build()

    assert timeseries_entry.ts in msg.timeseries
    assert msg.timeseries[timeseries_entry.ts] == (timeseries_entry, entry2)
    assert msg.timeseries_datapoint_count() == 2
    assert msg.size == DEFAULT_FIELDS_SIZE + timeseries_entry.size + entry2.size


def test_add_timeseries_with_none_ts():
    entry = TimeseriesEntry(key="x", value=1, ts=None)
    builder = DeviceUplinkMessageBuilder()
    msg = builder.add_timeseries(entry).build()

    assert 0 in msg.timeseries
    assert msg.timeseries[0] == (entry,)
    assert msg.timeseries_datapoint_count() == 1
    assert msg.size == DEFAULT_FIELDS_SIZE + entry.size


def test_add_timeseries_from_ordered_dict(timeseries_entry):
    ordered = OrderedDict({timeseries_entry.ts: [timeseries_entry]})
    builder = DeviceUplinkMessageBuilder()
    msg = builder.add_timeseries(ordered).build()

    assert msg.timeseries[timeseries_entry.ts] == (timeseries_entry,)
    assert msg.timeseries_datapoint_count() == 1


def test_add_delivery_futures():
    future1 = asyncio.Future()
    future2 = asyncio.Future()
    builder = DeviceUplinkMessageBuilder()
    msg = builder.add_delivery_futures([future1, future2]).build()

    assert msg.delivery_futures == (future1, future2)


def test_repr_contains_info(attribute_entry, timeseries_entry):
    builder = DeviceUplinkMessageBuilder()
    msg = builder.set_device_name("test-device") \
        .set_device_profile("dp") \
        .add_attributes(attribute_entry) \
        .add_timeseries(timeseries_entry) \
        .build()

    repr_str = repr(msg)
    assert "test-device" in repr_str
    assert "dp" in repr_str
    assert "AttributeEntry" in repr_str
    assert "TimeseriesEntry" in repr_str
