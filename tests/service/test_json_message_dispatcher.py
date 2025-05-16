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
from tb_mqtt_client.service.message_dispatcher import JsonMessageDispatcher
from tb_mqtt_client.constants.mqtt_topics import DEVICE_TELEMETRY_TOPIC, DEVICE_ATTRIBUTES_TOPIC
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry


@pytest.fixture
def dispatcher():
    return JsonMessageDispatcher(max_payload_size=512, max_datapoints=10)


def test_single_telemetry_dispatch(dispatcher):
    builder = DeviceUplinkMessageBuilder().set_device_name("dev1")
    builder.add_telemetry(TimeseriesEntry("temp", 25))
    msg = builder.build()

    payloads = dispatcher.build_topic_payloads([msg])
    assert len(payloads) == 1
    topic, payload, count = payloads[0]
    assert topic == DEVICE_TELEMETRY_TOPIC
    assert b"dev1" in payload
    assert count == 1


def test_single_attribute_dispatch(dispatcher):
    builder = DeviceUplinkMessageBuilder().set_device_name("dev2")
    builder.add_attributes(AttributeEntry("mode", "auto"))
    msg = builder.build()

    payloads = dispatcher.build_topic_payloads([msg])
    assert len(payloads) == 1
    topic, payload, count = payloads[0]
    assert topic == DEVICE_ATTRIBUTES_TOPIC
    assert b"dev2" in payload
    assert count == 1


def test_multiple_devices_grouping(dispatcher):
    b1 = DeviceUplinkMessageBuilder().set_device_name("dev1")
    b1.add_telemetry(TimeseriesEntry("t1", 1))
    b2 = DeviceUplinkMessageBuilder().set_device_name("dev2")
    b2.add_telemetry(TimeseriesEntry("t2", 2))

    payloads = dispatcher.build_topic_payloads([b1.build(), b2.build()])
    assert len(payloads) == 2
    for topic, payload, count in payloads:
        assert topic == DEVICE_TELEMETRY_TOPIC
        assert count == 1


def test_large_telemetry_split(dispatcher):
    builder = DeviceUplinkMessageBuilder().set_device_name("splittest")
    for i in range(15):
        builder.add_telemetry(TimeseriesEntry(f"key{i}", i))

    payloads = dispatcher.build_topic_payloads([builder.build()])
    assert len(payloads) > 1
    for topic, payload, count in payloads:
        assert topic == DEVICE_TELEMETRY_TOPIC
        assert count <= dispatcher.splitter.max_datapoints


def test_large_attributes_split():
    dispatcher = JsonMessageDispatcher(max_payload_size=200)

    builder = DeviceUplinkMessageBuilder().set_device_name("splitattr")
    for i in range(20):
        builder.add_attributes(AttributeEntry(f"k{i}", "x" * 50))  # Increase size

    payloads = dispatcher.build_topic_payloads([builder.build()])
    assert len(payloads) > 1  # Now expect splitting
    for topic, payload, count in payloads:
        assert topic == DEVICE_ATTRIBUTES_TOPIC
        assert count > 0