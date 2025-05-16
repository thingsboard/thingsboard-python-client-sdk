#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#


import pytest
from tb_mqtt_client.service.message_splitter import MessageSplitter
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry


@pytest.mark.parametrize("max_payload_size,max_datapoints", [(100, 3)])
def test_split_large_telemetry(max_payload_size, max_datapoints):
    splitter = MessageSplitter(max_payload_size=max_payload_size, max_datapoints=max_datapoints)

    builder = DeviceUplinkMessageBuilder().set_device_name("device1")
    for i in range(10):
        builder.add_telemetry(TimeseriesEntry(f"k{i}", i))

    message = builder.build()
    split = splitter.split_timeseries([message])

    assert len(split) > 1
    total_ts = sum(len(m.timeseries) for m in split)
    assert total_ts == 10
    for m in split:
        assert m.device_name == "device1"


def test_split_large_attributes():
    splitter = MessageSplitter(max_payload_size=100)

    builder = DeviceUplinkMessageBuilder().set_device_name("deviceA")
    for i in range(20):
        builder.add_attributes(AttributeEntry(f"attr{i}", "val" * 10))

    message = builder.build()
    split = splitter.split_attributes([message])

    assert len(split) > 1
    total_attrs = sum(len(m.attributes) for m in split)
    assert total_attrs == 20
    for m in split:
        assert m.device_name == "deviceA"


def test_no_split_needed():
    splitter = MessageSplitter(max_payload_size=10000, max_datapoints=100)

    builder = DeviceUplinkMessageBuilder().set_device_name("simpleDevice")
    builder.add_telemetry(TimeseriesEntry("temp", 23))
    builder.add_attributes(AttributeEntry("fw", "1.0.0"))

    message = builder.build()
    result_attr = splitter.split_attributes([message])
    result_ts = splitter.split_timeseries([message])

    assert len(result_attr) == 1
    assert len(result_ts) == 1
    assert result_attr[0].device_name == "simpleDevice"
    assert result_ts[0].device_name == "simpleDevice"
    assert len(result_attr[0].attributes) == 1
    assert len(result_ts[0].timeseries) == 1


def test_mixed_split():
    splitter = MessageSplitter(max_payload_size=120, max_datapoints=2)
    builder = DeviceUplinkMessageBuilder().set_device_name("mixed")

    # Increase attribute value size to ensure payload size > 120
    for i in range(5):
        builder.add_attributes(AttributeEntry(f"a{i}", "x" * 50))
        builder.add_telemetry(TimeseriesEntry(f"t{i}", i))

    msg = builder.build()
    result_attr = splitter.split_attributes([msg])
    result_ts = splitter.split_timeseries([msg])

    # Assert that the attributes and telemetry are split into multiple messages
    assert len(result_attr) > 1, f"Expected split, got {len(result_attr)}"
    assert len(result_ts) > 1, f"Expected split, got {len(result_ts)}"
    assert sum(len(r.attributes) for r in result_attr) == 5
    assert sum(len(r.timeseries) for r in result_ts) == 5
