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


from datetime import datetime, UTC

import orjson
import pytest

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.constants.mqtt_topics import GATEWAY_TELEMETRY_TOPIC, GATEWAY_ATTRIBUTES_TOPIC, \
    GATEWAY_CONNECT_TOPIC, GATEWAY_DISCONNECT_TOPIC, GATEWAY_ATTRIBUTES_REQUEST_TOPIC, GATEWAY_RPC_TOPIC, \
    GATEWAY_CLAIM_TOPIC
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.device_connect_message import DeviceConnectMessage
from tb_mqtt_client.entities.gateway.device_disconnect_message import DeviceDisconnectMessage
from tb_mqtt_client.entities.gateway.device_info import DeviceInfo
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_attribute_update import GatewayAttributeUpdate
from tb_mqtt_client.entities.gateway.gateway_claim_request import GatewayClaimRequestBuilder
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.entities.gateway.gateway_uplink_message import GatewayUplinkMessageBuilder, \
    DEFAULT_FIELDS_SIZE
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.service.gateway.message_adapter import JsonGatewayMessageAdapter


def test_init():
    # Setup & Act
    adapter = JsonGatewayMessageAdapter(max_payload_size=1000, max_datapoints=100)
    
    # Assert
    assert adapter.splitter.max_payload_size == 1000 - DEFAULT_FIELDS_SIZE
    assert adapter.splitter.max_datapoints == 100


def test_build_device_connect_message_payload():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    device_connect_message = DeviceConnectMessage.build("test_device", "default")
    
    # Act
    result = adapter.build_device_connect_message_payload(device_connect_message, qos=1)
    
    # Assert
    assert isinstance(result, MqttPublishMessage)
    assert result.topic == GATEWAY_CONNECT_TOPIC
    assert result.qos == 1
    
    # Verify payload content
    payload_dict = orjson.loads(result.payload)
    assert "device" in payload_dict
    assert payload_dict["device"] == "test_device"
    assert "type" in payload_dict
    assert payload_dict["type"] == "default"


def test_build_device_disconnect_message_payload():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    device_disconnect_message = DeviceDisconnectMessage.build("test_device")
    
    # Act
    result = adapter.build_device_disconnect_message_payload(device_disconnect_message, qos=1)
    
    # Assert
    assert isinstance(result, MqttPublishMessage)
    assert result.topic == GATEWAY_DISCONNECT_TOPIC
    assert result.qos == 1
    
    # Verify payload content
    payload_dict = orjson.loads(result.payload)
    assert "device" in payload_dict
    assert payload_dict["device"] == "test_device"


@pytest.mark.asyncio
async def test_build_gateway_attribute_request_payload():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    attribute_request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])
    
    # Act
    result = adapter.build_gateway_attribute_request_payload(attribute_request, qos=1)
    
    # Assert
    assert isinstance(result, MqttPublishMessage)
    assert result.topic == GATEWAY_ATTRIBUTES_REQUEST_TOPIC
    assert result.qos == 1
    
    # Verify payload content
    payload_dict = orjson.loads(result.payload)
    assert "device" in payload_dict
    assert payload_dict["device"] == "test_device"
    assert "keys" in payload_dict
    assert payload_dict["keys"] == ["key1", "key2"]
    assert "id" in payload_dict
    assert payload_dict["id"] == attribute_request.request_id


def test_build_rpc_response_payload():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    rpc_response = GatewayRPCResponse.build(device_name="test_device", request_id=1, result="success")
    
    # Act
    result = adapter.build_rpc_response_payload(rpc_response, qos=1)
    
    # Assert
    assert isinstance(result, MqttPublishMessage)
    assert result.topic == GATEWAY_RPC_TOPIC
    assert result.qos == 1
    
    # Verify payload content
    payload_dict = orjson.loads(result.payload)
    assert "device" in payload_dict
    assert payload_dict["device"] == "test_device"
    assert "id" in payload_dict
    assert payload_dict["id"] == 1
    assert "data" in payload_dict
    assert payload_dict["data"] == {"result": "success"}


def test_build_claim_request_payload():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    device_claim_request = ClaimRequest.build(secret_key="secret", duration=1)
    gateway_claim_request = GatewayClaimRequestBuilder().add_device_request(device_name_or_session="test_device",
                                                                            device_claim_request=device_claim_request).build()
    
    # Act
    result = adapter.build_claim_request_payload(gateway_claim_request, qos=1)
    
    # Assert
    assert isinstance(result, MqttPublishMessage)
    assert result.topic == GATEWAY_CLAIM_TOPIC
    assert result.qos == 1
    
    # Verify payload content
    payload_dict = orjson.loads(result.payload)
    assert "test_device" in payload_dict
    assert payload_dict["test_device"]["secretKey"] == "secret"
    assert payload_dict["test_device"]["durationMs"] == 1000


def test_parse_attribute_update():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    data = {
        "device": "test_device",
        "data": {
            "key1": "value1",
            "key2": 42
        }
    }
    
    # Act
    result = adapter.parse_attribute_update(data)
    
    # Assert
    assert isinstance(result, GatewayAttributeUpdate)
    assert result.device_name == "test_device"
    assert isinstance(result.attribute_update, AttributeUpdate)
    assert result.attribute_update.as_dict() == {"key1": "value1", "key2": 42}


def test_parse_attribute_update_invalid_format():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    data = {
        "invalid_format": True
    }
    
    # Act & Assert
    with pytest.raises(ValueError):
        adapter.parse_attribute_update(data)


@pytest.mark.asyncio
async def test_parse_gateway_requested_attribute_response():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    attribute_request = await GatewayAttributeRequest.build(device_session=device_session, client_keys=["key1", "key2"])
    
    data = {
        "device": "test_device",
        "id": attribute_request.request_id,
        "values": {
            "key1": "value1",
            "key2": 42
        }
    }
    
    # Act
    result = adapter.parse_gateway_requested_attribute_response(attribute_request, data)
    
    # Assert
    assert isinstance(result, GatewayRequestedAttributeResponse)
    assert result.device_name == "test_device"
    assert result.request_id == attribute_request.request_id
    assert len(result.client) == 2
    assert any(attr.key == "key1" and attr.value == "value1" for attr in result.client)
    assert any(attr.key == "key2" and attr.value == 42 for attr in result.client)
    assert len(result.shared) == 0


@pytest.mark.asyncio
async def test_parse_gateway_requested_attribute_response_single_value():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    attribute_request = await GatewayAttributeRequest.build(device_session=device_session, client_keys=["key1"])
    
    data = {
        "device": "test_device",
        "id": attribute_request.request_id,
        "value": "value1"
    }
    
    # Act
    result = adapter.parse_gateway_requested_attribute_response(attribute_request, data)
    
    # Assert
    assert isinstance(result, GatewayRequestedAttributeResponse)
    assert result.device_name == "test_device"
    assert result.request_id == attribute_request.request_id
    assert len(result.client) == 1
    assert result.client[0].key == "key1"
    assert result.client[0].value == "value1"
    assert len(result.shared) == 0


@pytest.mark.asyncio
async def test_parse_gateway_requested_attribute_response_shared_keys():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    device_info = DeviceInfo("test_device", "default")
    device_session = DeviceSession(device_info)
    attribute_request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["key1", "key2"])
    
    data = {
        "device": "test_device",
        "id": attribute_request.request_id,
        "values": {
            "key1": "value1",
            "key2": 42
        }
    }
    
    # Act
    result = adapter.parse_gateway_requested_attribute_response(attribute_request, data)
    
    # Assert
    assert isinstance(result, GatewayRequestedAttributeResponse)
    assert result.device_name == "test_device"
    assert result.request_id == attribute_request.request_id
    assert len(result.shared) == 2
    assert any(attr.key == "key1" and attr.value == "value1" for attr in result.shared)
    assert any(attr.key == "key2" and attr.value == 42 for attr in result.shared)
    assert len(result.client) == 0


def test_parse_rpc_request():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    data = {
        "device": "test_device",
        "data": {
            "id": 1,
            "method": "test_method",
            "params": {
                "param1": "value1",
                "param2": 42
            }
        }
    }
    
    # Act
    result = adapter.parse_rpc_request("v1/gateway/rpc", data)
    
    # Assert
    assert isinstance(result, GatewayRPCRequest)
    assert result.device_name == "test_device"
    assert result.request_id == 1
    assert result.method == "test_method"
    assert result.params == {"param1": "value1", "param2": 42}


def test_parse_rpc_request_invalid_format():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    data = {
        "invalid_format": True
    }
    
    # Act & Assert
    with pytest.raises(ValueError):
        adapter.parse_rpc_request("v1/gateway/rpc", data)


def test_deserialize_to_dict():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    payload = b'{"key": "value", "number": 42}'
    
    # Act
    result = adapter.deserialize_to_dict(payload)
    
    # Assert
    assert isinstance(result, dict)
    assert result == {"key": "value", "number": 42}


def test_deserialize_to_dict_invalid_format():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    payload = b'invalid json'
    
    # Act & Assert
    with pytest.raises(ValueError):
        adapter.deserialize_to_dict(payload)


def test_pack_attributes():
    # Setup
    attributes = [
        AttributeEntry("key1", "value1"),
        AttributeEntry("key2", 42)
    ]
    uplink_message = GatewayUplinkMessageBuilder().set_device_name("test_device").add_attributes(attributes).build()
    
    # Act
    result = JsonGatewayMessageAdapter.pack_attributes(uplink_message)
    
    # Assert
    assert isinstance(result, dict)
    assert result == {"key1": "value1", "key2": 42}


def test_pack_timeseries_no_timestamp():
    # Setup
    timeseries = [TimeseriesEntry("temp", 22.5), TimeseriesEntry("humidity", 45)]
    uplink_message = GatewayUplinkMessageBuilder().set_device_name("test_device").add_timeseries(timeseries).build()
    
    # Act
    result = JsonGatewayMessageAdapter.pack_timeseries(uplink_message)
    
    # Assert
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == {"temp": 22.5, "humidity": 45}


def test_pack_timeseries_with_timestamp():
    # Setup
    ts = int(datetime.now(UTC).timestamp() * 1000)
    timeseries = [TimeseriesEntry("temp", 22.5, ts), TimeseriesEntry("humidity", 45, ts)]
    uplink_message = GatewayUplinkMessageBuilder().set_device_name("test_device").add_timeseries(timeseries).build()
    
    # Act
    result = JsonGatewayMessageAdapter.pack_timeseries(uplink_message)
    
    # Assert
    assert isinstance(result, list)
    assert len(result) == 1
    assert "ts" in result[0]
    assert result[0]["ts"] == ts
    assert "values" in result[0]
    assert result[0]["values"] == {"temp": 22.5, "humidity": 45}


def test_pack_timeseries_with_different_timestamps():
    # Setup
    ts1 = int(datetime.now(UTC).timestamp() * 1000)
    ts2 = ts1 + 1000  # 1 second later
    timeseries = [TimeseriesEntry("temp", 22.5, ts1), TimeseriesEntry("humidity", 45, ts2)]
    uplink_message = GatewayUplinkMessageBuilder().set_device_name("test_device").add_timeseries(timeseries).build()
    
    # Act
    result = JsonGatewayMessageAdapter.pack_timeseries(uplink_message)
    
    # Assert
    assert isinstance(result, list)
    assert len(result) == 2
    
    # Find entries by timestamp
    ts1_entry = next((entry for entry in result if entry["ts"] == ts1), None)
    ts2_entry = next((entry for entry in result if entry["ts"] == ts2), None)
    
    assert ts1_entry is not None
    assert ts2_entry is not None
    assert ts1_entry["values"] == {"temp": 22.5}
    assert ts2_entry["values"] == {"humidity": 45}


def test_build_uplink_messages_empty():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    
    # Act
    result = adapter.build_uplink_messages([])
    
    # Assert
    assert result == []


def test_build_uplink_messages_non_gateway_message():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    mqtt_msg = MqttPublishMessage(topic="test/topic", payload=b"test_payload", qos=1)
    
    # Act
    result = adapter.build_uplink_messages([mqtt_msg])
    
    # Assert
    assert len(result) == 1
    assert result[0] == mqtt_msg


def test_build_uplink_messages_with_telemetry():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    
    # Create a gateway uplink message with telemetry
    uplink_message = (GatewayUplinkMessageBuilder()
                      .set_device_name("test_device")
                      .add_timeseries([TimeseriesEntry("temp", 22.5), TimeseriesEntry("humidity", 45)])
                      .build())
    
    mqtt_msg = MqttPublishMessage(topic="test/topic", payload=uplink_message, qos=1)
    
    # Act
    result = adapter.build_uplink_messages([mqtt_msg])
    
    # Assert
    assert len(result) == 1
    assert result[0].topic == GATEWAY_TELEMETRY_TOPIC
    assert result[0].qos == 1
    
    # Verify payload content
    payload_dict = orjson.loads(result[0].payload)
    assert "test_device" in payload_dict
    assert isinstance(payload_dict["test_device"], list)
    assert len(payload_dict["test_device"]) == 1
    assert "values" in payload_dict["test_device"][0]
    assert payload_dict["test_device"][0]["values"] == {"temp": 22.5, "humidity": 45}


def test_build_uplink_messages_with_attributes():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    
    # Create a gateway uplink message with attributes
    uplink_message = GatewayUplinkMessageBuilder().set_device_name("test_device").add_attributes([
        AttributeEntry("key1", "value1"),
        AttributeEntry("key2", 42)
    ]).build()
    
    mqtt_msg = MqttPublishMessage(topic="test/topic", payload=uplink_message, qos=1)
    
    # Act
    result = adapter.build_uplink_messages([mqtt_msg])
    
    # Assert
    assert len(result) == 1
    assert result[0].topic == GATEWAY_ATTRIBUTES_TOPIC
    assert result[0].qos == 1
    
    # Verify payload content
    payload_dict = orjson.loads(result[0].payload)
    assert "test_device" in payload_dict
    assert payload_dict["test_device"] == {"key1": "value1", "key2": 42}


def test_build_uplink_messages_with_both_telemetry_and_attributes():
    # Setup
    adapter = JsonGatewayMessageAdapter()
    
    # Create a gateway uplink message with both telemetry and attributes
    uplink_message_builder = GatewayUplinkMessageBuilder().set_device_name("test_device")
    uplink_message_builder.add_timeseries([TimeseriesEntry("temp", 22.5), TimeseriesEntry("humidity", 45)])
    uplink_message_builder.add_attributes([AttributeEntry("key1", "value1"), AttributeEntry("key2", 42)])
    uplink_message = uplink_message_builder.build()
    
    mqtt_msg = MqttPublishMessage(topic="test/topic", payload=uplink_message, qos=1)
    
    # Act
    result = adapter.build_uplink_messages([mqtt_msg])
    
    # Assert
    assert len(result) == 2
    
    # Find telemetry and attribute messages
    telemetry_msg = next((msg for msg in result if msg.topic == GATEWAY_TELEMETRY_TOPIC), None)
    attribute_msg = next((msg for msg in result if msg.topic == GATEWAY_ATTRIBUTES_TOPIC), None)
    
    assert telemetry_msg is not None
    assert attribute_msg is not None
    
    # Verify telemetry payload
    telemetry_dict = orjson.loads(telemetry_msg.payload)
    assert "test_device" in telemetry_dict
    assert isinstance(telemetry_dict["test_device"], list)
    assert len(telemetry_dict["test_device"]) == 1
    assert "values" in telemetry_dict["test_device"][0]
    assert telemetry_dict["test_device"][0]["values"] == {"temp": 22.5, "humidity": 45}
    
    # Verify attribute payload
    attribute_dict = orjson.loads(attribute_msg.payload)
    assert "test_device" in attribute_dict
    assert attribute_dict["test_device"] == {"key1": "value1", "key2": 42}


if __name__ == '__main__':
    pytest.main([__file__])
