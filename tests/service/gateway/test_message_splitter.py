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

from tb_mqtt_client.common.logging_utils import configure_logging
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessageBuilder, \
    DEFAULT_FIELDS_SIZE
from tb_mqtt_client.service.gateway.message_splitter import GatewayMessageSplitter

configure_logging()


def test_init_default():
    # Setup & Act
    splitter = GatewayMessageSplitter()

    # Assert
    assert splitter.max_payload_size == 55000 - DEFAULT_FIELDS_SIZE
    assert splitter.max_datapoints == 0


def test_init_custom():
    # Setup & Act
    splitter = GatewayMessageSplitter(max_payload_size=10000, max_datapoints=100)

    # Assert
    assert splitter.max_payload_size == 10000 - DEFAULT_FIELDS_SIZE
    assert splitter.max_datapoints == 100


def test_init_invalid_values():
    # Setup & Act
    splitter = GatewayMessageSplitter(max_payload_size=-1, max_datapoints=-1)

    # Assert
    assert splitter.max_payload_size == 55000 - DEFAULT_FIELDS_SIZE  # Default value
    assert splitter.max_datapoints == 0  # Default value


def test_max_payload_size_property():
    # Setup
    splitter = GatewayMessageSplitter()

    # Act
    splitter.max_payload_size = 20000

    # Assert
    assert splitter.max_payload_size == 20000 - DEFAULT_FIELDS_SIZE


def test_max_payload_size_property_invalid():
    # Setup
    splitter = GatewayMessageSplitter()

    # Act
    splitter.max_payload_size = -1

    # Assert
    assert splitter.max_payload_size == 55000 - DEFAULT_FIELDS_SIZE  # Default value


def test_max_datapoints_property():
    # Setup
    splitter = GatewayMessageSplitter()

    # Act
    splitter.max_datapoints = 200

    # Assert
    assert splitter.max_datapoints == 200


def test_max_datapoints_property_invalid():
    # Setup
    splitter = GatewayMessageSplitter()

    # Act
    splitter.max_datapoints = -1

    # Assert
    assert splitter.max_datapoints == 0  # Default value


@pytest.mark.asyncio
async def test_split_timeseries_no_split_needed():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=10000, max_datapoints=100)

    # Create a message with timeseries that doesn't need splitting
    timeseries = TimeseriesEntry("temp", 22.5)
    message = GatewayUplinkMessageBuilder().set_device_name("test_device").add_timeseries(timeseries).build()

    # Act
    result = splitter.split_timeseries([message])

    # Assert
    assert len(result) == 1
    assert result[0] == message


@pytest.mark.asyncio
async def test_split_timeseries_by_size():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=100 + DEFAULT_FIELDS_SIZE, max_datapoints=100)

    # Create a message with timeseries that needs splitting by size
    message_builder = GatewayUplinkMessageBuilder().set_device_name("test_device")

    # Mock the size property to force splitting
    timeseries_entry = TimeseriesEntry("temp", 22.5)
    entries = [TimeseriesEntry("temp", 22.5) for _ in range(int(60 / timeseries_entry.size + 1))]
    entries.extend([TimeseriesEntry("humidity", 45) for _ in range(int(60 / timeseries_entry.size + 1))])
    message_builder.add_timeseries(entries)
    message = message_builder.build()

    # Mock the event loop
    loop_mock = MagicMock()
    future = asyncio.Future()
    loop_mock.create_future.return_value = future

    with patch('asyncio.get_running_loop', return_value=loop_mock):
        # Act
        result = splitter.split_timeseries([message])

        # Assert
        assert len(result) == 2
        assert result[0].device_name == "test_device"
        assert result[1].device_name == "test_device"

        # Check that each result has only one of the timeseries entries
        assert result[0].size - DEFAULT_FIELDS_SIZE < splitter.max_payload_size
        assert result[1].size - DEFAULT_FIELDS_SIZE < splitter.max_payload_size


@pytest.mark.asyncio
async def test_split_timeseries_by_datapoints():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=10000, max_datapoints=1)

    # Create a message with timeseries that needs splitting by datapoints
    message_builder = GatewayUplinkMessageBuilder().set_device_name("test_device")
    entries = [
        TimeseriesEntry("temp", 22.5),
        TimeseriesEntry("humidity", 45)
    ]
    message_builder.add_timeseries(entries)
    message = message_builder.build()

    # Mock the event loop
    loop_mock = MagicMock()
    future = asyncio.Future()
    loop_mock.create_future.return_value = future

    with patch('asyncio.get_running_loop', return_value=loop_mock):
        # Act
        result = splitter.split_timeseries([message])

        # Assert
        assert len(result) == 2
        assert result[0].device_name == "test_device"
        assert result[1].device_name == "test_device"

        # Check that each result has only one of the timeseries entries
        assert len(result[0].timeseries[0]) == 1
        assert len(result[1].timeseries[0]) == 1

        # The entries should be in separate messages
        key0 = result[0].timeseries[0][0].key
        key1 = result[1].timeseries[0][0].key
        assert key0 != key1
        assert key0 == 'temp'
        assert key1 == 'humidity'


@pytest.mark.asyncio
async def test_split_timeseries_multiple_messages():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=10000, max_datapoints=100)

    # Create multiple messages with timeseries
    message1 = GatewayUplinkMessageBuilder().set_device_name("device1").add_timeseries(
        TimeseriesEntry("temp", 22.5)
    ).build()

    message2 = GatewayUplinkMessageBuilder().set_device_name("device2").add_timeseries(
        TimeseriesEntry("humidity", 45)
    ).build()

    # Act
    result = splitter.split_timeseries([message1, message2])

    # Assert
    assert len(result) == 2

    # Check that the messages are for different devices
    devices = {msg.device_name for msg in result}
    assert devices == {"device1", "device2"}

    # Check that each result has the correct timeseries
    for msg in result:
        if msg.device_name == "device1":
            assert "temp" == msg.timeseries[0][0].key
        elif msg.device_name == "device2":
            assert "humidity" == msg.timeseries[0][0].key


@pytest.mark.asyncio
async def test_split_timeseries_with_delivery_futures():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=100 + DEFAULT_FIELDS_SIZE, max_datapoints=100)

    # Create a message with enough timeseries entries to exceed the max size
    message_builder = GatewayUplinkMessageBuilder().set_device_name("test_device")
    sample_entry = TimeseriesEntry("temp", 22.5)
    # Create enough "temp" and "humidity" entries to force splitting
    entries = [TimeseriesEntry("temp", 22.5) for _ in range(int(60 / sample_entry.size + 1))]
    entries.extend([TimeseriesEntry("humidity", 45) for _ in range(int(60 / sample_entry.size + 1))])
    message_builder.add_timeseries(entries)

    # Add a delivery future to the original message
    parent_future = asyncio.Future()
    message_builder.add_delivery_futures([parent_future])

    message = message_builder.build()

    # Mock the event loop and future_map
    loop_mock = MagicMock()
    future = asyncio.Future()
    loop_mock.create_future.return_value = future

    with patch('asyncio.get_running_loop', return_value=loop_mock), \
            patch('tb_mqtt_client.common.async_utils.future_map.register') as mock_register:
        # Act
        result = splitter.split_timeseries([message])

        # Assert
        assert len(result) == 2
        # Ensure futures are linked correctly
        assert mock_register.call_count == 2
        mock_register.assert_any_call(parent_future, [future])


@pytest.mark.asyncio
async def test_split_attributes_no_split_needed():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=10000, max_datapoints=100)

    # Create a message with attributes that doesn't need splitting
    message = (GatewayUplinkMessageBuilder()
               .set_device_name("test_device")
               .add_attributes(AttributeEntry("key1", "value1"))
               .build())

    # Act
    result = splitter.split_attributes([message])

    # Assert
    assert len(result) == 1
    assert result[0] == message


@pytest.mark.asyncio
async def test_split_attributes_by_size():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=100 + DEFAULT_FIELDS_SIZE, max_datapoints=100)

    # Enough repeated attributes to exceed 100 bytes (simulate large payload)
    attrs = [AttributeEntry(f"key{i}", f"value{i}") for i in range(10)]

    message = GatewayUplinkMessageBuilder().set_device_name("test_device").add_attributes(attrs).build()

    # Mock the event loop
    loop_mock = MagicMock()
    future = asyncio.Future()
    loop_mock.create_future.return_value = future

    with patch('asyncio.get_running_loop', return_value=loop_mock):
        # Act
        result = splitter.split_attributes([message])

        # Assert
        assert len(result) > 1
        # Ensure each split contains fewer attributes than the original
        for part in result:
            assert len(part.attributes) < len(attrs)


@pytest.mark.asyncio
async def test_split_attributes_by_datapoints():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=10000, max_datapoints=1)

    # Create a message with attributes that needs splitting by datapoints
    message = (GatewayUplinkMessageBuilder()
               .set_device_name("test_device")
               .add_attributes([AttributeEntry("key1", "value1"),
                                AttributeEntry("key2", "value2")])
               .build())

    # Mock the event loop
    loop_mock = MagicMock()
    future = asyncio.Future()
    loop_mock.create_future.return_value = future

    with patch('asyncio.get_running_loop', return_value=loop_mock):
        # Act
        result = splitter.split_attributes([message])

        # Assert
        assert len(result) == 2
        assert result[0].device_name == "test_device"
        assert result[1].device_name == "test_device"

        # Check that each result has only one of the attribute entries
        assert len(result[0].attributes) == 1
        assert len(result[1].attributes) == 1

        # The entries should be in separate messages
        keys0 = {attr.key for attr in result[0].attributes}
        keys1 = {attr.key for attr in result[1].attributes}
        assert len(keys0.intersection(keys1)) == 0
        assert keys0.union(keys1) == {"key1", "key2"}


@pytest.mark.asyncio
async def test_split_attributes_multiple_messages():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=10000, max_datapoints=100)

    # Create multiple messages with attributes
    message1 = (GatewayUplinkMessageBuilder()
                .set_device_name("device1")
                .add_attributes(AttributeEntry("key1", "value1"))
                .build())

    message2 = (GatewayUplinkMessageBuilder()
                .set_device_name("device2")
                .add_attributes(AttributeEntry("key2", "value2"))
                .build())

    # Act
    result = splitter.split_attributes([message1, message2])

    # Assert
    assert len(result) == 2

    # Check that the messages are for different devices
    devices = {msg.device_name for msg in result}
    assert devices == {"device1", "device2"}

    # Check that each result has the correct attributes
    for msg in result:
        if msg.device_name == "device1":
            assert msg.attributes[0].key == "key1"
        elif msg.device_name == "device2":
            assert msg.attributes[0].key == "key2"


@pytest.mark.asyncio
async def test_split_attributes_with_delivery_futures():
    # Setup
    splitter = GatewayMessageSplitter(max_payload_size=100 + DEFAULT_FIELDS_SIZE, max_datapoints=100)

    # Create enough attributes to exceed the max size
    attrs = [AttributeEntry(f"key{i}", f"value{i}") for i in range(10)]

    # Delivery future for the original message
    parent_future = asyncio.Future()

    message = (GatewayUplinkMessageBuilder()
               .set_device_name("test_device")
               .add_attributes(attrs)
               .add_delivery_futures([parent_future])
               .build())

    # Mock the event loop and future_map
    loop_mock = MagicMock()
    future = asyncio.Future()
    loop_mock.create_future.return_value = future

    with patch('asyncio.get_running_loop', return_value=loop_mock), \
            patch('tb_mqtt_client.common.async_utils.future_map.register') as mock_register:
        # Act
        result = splitter.split_attributes([message])

        # Assert
        assert len(result) > 1
        assert mock_register.call_count == len(result)
        mock_register.assert_any_call(parent_future, [future])


if __name__ == '__main__':
    pytest.main([__file__])
