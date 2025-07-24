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
from unittest.mock import MagicMock, patch

import pytest

from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.message_adapter import JsonMessageAdapter
from tb_mqtt_client.service.message_splitter import MessageSplitter


@pytest.fixture
def splitter():
    return MessageSplitter(max_payload_size=100, max_datapoints=3)


# Positive cases
def test_single_small_timeseries_pass_through(splitter):
    msg = MagicMock()
    msg.has_timeseries.return_value = True
    msg.size = 50
    msg.attributes_datapoint_count.return_value = 0
    msg.timeseries_datapoint_count.return_value = 2
    result = splitter.split_timeseries([msg])
    assert result == [msg]


def test_single_small_attributes_pass_through(splitter):
    msg = MagicMock()
    msg.has_attributes.return_value = True
    msg.size = 50
    msg.attributes_datapoint_count.return_value = 2
    msg.timeseries_datapoint_count.return_value = 0
    result = splitter.split_attributes([msg])
    assert result == [msg]


# Negative test: invalid payload size and datapoints
def test_invalid_config_defaults():
    splitter = MessageSplitter(max_payload_size=-10, max_datapoints=-100)
    assert splitter.max_payload_size == 65535
    assert splitter.max_datapoints == 0


# Negative test: empty message list
def test_empty_list_returns_empty(splitter):
    assert splitter.split_timeseries([]) == []
    assert splitter.split_attributes([]) == []


# Negative test: message without required fields
def test_malformed_message_handling(splitter):
    msg_ts = MagicMock()
    msg_ts.has_timeseries.side_effect = Exception("Malformed TS field")
    msg_ts.attributes_datapoint_count.return_value = 0
    msg_ts.timeseries_datapoint_count.return_value = 0
    msg_ts.size = 200

    with pytest.raises(Exception, match="Malformed TS field"):
        splitter.split_timeseries([msg_ts])

    msg_attr = MagicMock()
    msg_attr.has_attributes.side_effect = Exception("Malformed Attr field")
    msg_attr.attributes_datapoint_count.return_value = 0
    msg_attr.timeseries_datapoint_count.return_value = 0
    msg_attr.size = 200

    with pytest.raises(Exception, match="Malformed Attr field"):
        splitter.split_attributes([msg_attr])


# Negative test: builder fails on build()
@patch("tb_mqtt_client.service.message_splitter.DeviceUplinkMessageBuilder")
def test_builder_failure_during_split_raises(mock_builder_class):
    entry = MagicMock()
    entry.size = 10

    message = MagicMock()
    message.device_name = "dev"
    message.device_profile = "prof"
    message.has_timeseries.return_value = True
    message.timeseries = {"temp": [entry] * 4}
    message.get_delivery_futures.return_value = []
    message.attributes_datapoint_count.return_value = 0
    message.timeseries_datapoint_count.return_value = 4
    message.size = 50

    builder_instance = MagicMock()
    builder_instance.set_device_name.return_value = builder_instance
    builder_instance.set_device_profile.return_value = builder_instance
    builder_instance.add_timeseries.return_value = None
    builder_instance._timeseries = [entry]
    builder_instance.build.side_effect = RuntimeError("build failed")
    mock_builder_class.return_value = builder_instance

    splitter = MessageSplitter(max_payload_size=20, max_datapoints=2)

    with pytest.raises(RuntimeError, match="build failed"):
        splitter.split_timeseries([message])


# Property validation
def test_payload_setter_validation():
    s = MessageSplitter()
    s.max_payload_size = 12345
    assert s.max_payload_size == 12345
    s.max_payload_size = 0
    assert s.max_payload_size == 65535


def test_datapoint_setter_validation():
    s = MessageSplitter()
    s.max_datapoints = 99
    assert s.max_datapoints == 99
    s.max_datapoints = 0
    assert s.max_datapoints == 0
    s.max_datapoints = -5
    assert s.max_datapoints == 0


@pytest.mark.asyncio
async def test_split_attributes_grouping():
    dispatcher = JsonMessageAdapter(max_payload_size=200, max_datapoints=5)

    builder1 = DeviceUplinkMessageBuilder().set_device_name("deviceA").set_device_profile("default")
    builder2 = DeviceUplinkMessageBuilder().set_device_name("deviceA").set_device_profile("default")

    for i in range(3):
        builder1.add_attributes(AttributeEntry(f"key_{i}", i))

    for i in range(3, 6):
        builder2.add_attributes(AttributeEntry(f"key_{i}", i))


    messages = [builder1.build(), builder2.build()]
    result = dispatcher.splitter.split_attributes(messages)

    assert len(result) == 2
    total_attrs = sum(len(msg.attributes) for msg in result)
    assert total_attrs == 6
    assert all(msg.device_name == "deviceA" for msg in result)
    for msg in result:
        for fut in msg.get_delivery_futures():
            fut.set_result(PublishResult("test/topic", 1, 1, 100, 0))


@pytest.mark.asyncio
async def test_split_attributes_different_devices_not_grouped():
    dispatcher = JsonMessageAdapter(max_payload_size=200, max_datapoints=100)

    builder1 = DeviceUplinkMessageBuilder().set_device_name("deviceA")
    builder2 = DeviceUplinkMessageBuilder().set_device_name("deviceB")

    for i in range(3):
        builder1.add_attributes(AttributeEntry(f"key_{i}", i))
        builder2.add_attributes(AttributeEntry(f"key_{i+3}", i+3))

    result = dispatcher.splitter.split_attributes([builder1.build(), builder2.build()])

    assert len(result) == 2
    assert result[0].device_name != result[1].device_name
    for msg in result:
        for fut in msg.get_delivery_futures():
            fut.set_result(PublishResult("test/topic", 1, 1, 100, 0))


@patch("tb_mqtt_client.service.message_splitter.future_map.register")
@pytest.mark.asyncio
async def test_split_timeseries_registers_futures_and_batches_correctly(mock_register):
    splitter = MessageSplitter(max_payload_size=100, max_datapoints=2)

    builder = DeviceUplinkMessageBuilder().set_device_name("deviceX").set_device_profile("profileY")
    entry1 = TimeseriesEntry("temp", 1, 100)
    entry2 = TimeseriesEntry("humidity", 2, 100)
    entry3 = TimeseriesEntry("pressure", 3, 100)

    builder.add_timeseries(entry1)
    builder.add_timeseries(entry2)
    builder.add_timeseries(entry3)

    parent_future = asyncio.Future()
    builder.add_delivery_futures(parent_future)
    original_msg = builder.build()

    result = splitter.split_timeseries([original_msg])

    # Should be split due to datapoints=3 > max_datapoints=2 → 2 batches
    assert len(result) == 2
    total_points = sum(m.timeseries_datapoint_count() for m in result)
    assert total_points == 3

    # Validate that register was called for the shared future for each batch
    assert mock_register.call_count == 2
    for call in mock_register.call_args_list:
        args, _ = call
        assert args[0] is parent_future
        assert isinstance(args[1], list)
        assert len(args[1]) == 1
        shared_future = args[1][0]
        assert isinstance(shared_future, asyncio.Future)
        assert hasattr(shared_future, "uuid")

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
