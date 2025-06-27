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

from unittest.mock import MagicMock, patch, mock_open

import pytest
from orjson import dumps

from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.constants.mqtt_topics import DEVICE_TELEMETRY_TOPIC, DEVICE_ATTRIBUTES_TOPIC
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.data.device_uplink_message import DeviceUplinkMessageBuilder
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, BasicProvisioningCredentials, \
    X509ProvisioningCredentials, AccessTokenProvisioningCredentials
from tb_mqtt_client.entities.data.provisioning_response import ProvisioningResponse
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.message_dispatcher import JsonMessageDispatcher


@pytest.fixture
def dummy_provisioning_request():
    credentials = AccessTokenProvisioningCredentials(
        provision_device_key="key",
        provision_device_secret="secret",
        access_token="token"
    )
    return ProvisioningRequest("some-device", credentials, device_name="dev", gateway=False)



def build_msg(device="devX", with_attr=False, with_ts=False):
    builder = DeviceUplinkMessageBuilder().set_device_name(device)
    if with_attr:
        builder.add_attributes(AttributeEntry("a", 1))
    if with_ts:
        builder.add_timeseries(TimeseriesEntry("t", 2, ts=1234567890))
    return builder.build()

@pytest.fixture
def dispatcher():
    return JsonMessageDispatcher()


def test_build_attribute_request(dispatcher):
    request = MagicMock(spec=AttributeRequest)
    request.request_id = 1
    request.to_payload_format.return_value = {"clientKeys": "temp", "sharedKeys": "shared"}
    topic, payload = dispatcher.build_attribute_request(request)
    assert topic.endswith("/1")
    assert b"clientKeys" in payload


def test_build_attribute_request_invalid(dispatcher):
    request = MagicMock(spec=AttributeRequest)
    request.request_id = None
    with pytest.raises(ValueError):
        dispatcher.build_attribute_request(request)


def test_build_claim_request(dispatcher):
    req = ClaimRequest.build("secretKey")
    topic, payload = dispatcher.build_claim_request(req)
    assert topic == mqtt_topics.DEVICE_CLAIM_TOPIC
    assert b"secretKey" in payload


def test_build_claim_request_invalid(dispatcher):
    with pytest.raises(ValueError):
        req = ClaimRequest.build(secret_key=None)  # Simulating an invalid request # noqa


def test_build_rpc_request(dispatcher):
    request = MagicMock(spec=RPCRequest)
    request.request_id = 42
    request.to_payload_format.return_value = {"method": "reboot"}
    topic, payload = dispatcher.build_rpc_request(request)
    assert topic.endswith("42")
    assert b"reboot" in payload


def test_build_rpc_request_invalid(dispatcher):
    request = MagicMock(spec=RPCRequest)
    request.request_id = None
    with pytest.raises(ValueError):
        dispatcher.build_rpc_request(request)


def test_build_rpc_response(dispatcher):
    response = MagicMock(spec=RPCResponse)
    response.request_id = 123
    response.to_payload_format.return_value = {"result": "ok"}
    topic, payload = dispatcher.build_rpc_response(response)
    assert topic.endswith("123")
    assert b"ok" in payload


def test_build_rpc_response_invalid(dispatcher):
    response = MagicMock(spec=RPCResponse)
    response.request_id = None
    with pytest.raises(ValueError):
        dispatcher.build_rpc_response(response)


def test_build_provision_request_access_token(dispatcher):
    credentials = AccessTokenProvisioningCredentials("key1", "secret1", access_token="tokenABC")
    req = ProvisioningRequest("localhost", credentials, device_name="dev1", gateway=True)
    topic, payload = dispatcher.build_provision_request(req)
    assert topic == mqtt_topics.PROVISION_REQUEST_TOPIC
    assert b"provisionDeviceKey" in payload
    assert b"tokenABC" in payload
    assert b"credentialsType" in payload
    assert b"deviceName" in payload
    assert b"gateway" in payload


def test_build_provision_request_mqtt_basic(dispatcher):
    credentials = BasicProvisioningCredentials("key2", "secret2", client_id="cid", username="user", password="pass")
    req = ProvisioningRequest("127.0.0.1", credentials, device_name="dev2", gateway=False)
    topic, payload = dispatcher.build_provision_request(req)
    assert b"clientId" in payload
    assert b"username" in payload
    assert b"password" in payload
    assert b"credentialsType" in payload


def test_build_provision_request_x509(dispatcher):
    cert_path = "/fake/path/cert.pem"
    cert_content = "-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----"
    with patch("builtins.open", mock_open(read_data=cert_content)):
        credentials = X509ProvisioningCredentials("key3", "secret3", "key.pem", cert_path, "ca.pem")
        req = ProvisioningRequest("iot.server", credentials, device_name="dev3")
        topic, payload = dispatcher.build_provision_request(req)
        assert b"hash" in payload
        assert b"credentialsType" in payload
        assert b"FAKECERT" in payload


def test_build_provision_request_x509_file_not_found(dispatcher):
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            X509ProvisioningCredentials("key", "secret", "k.pem", "nonexistent.pem", "ca.pem")


def test_parse_attribute_request_response(dispatcher):
    topic = "v1/devices/me/attributes/response/42"
    payload = dumps({"shared": {"temp": 22}})
    with patch.object(RequestedAttributeResponse, "from_dict", return_value="ok") as mock:
        result = dispatcher.parse_attribute_request_response(topic, payload)
        assert result == "ok"
        mock.assert_called_once()


def test_parse_attribute_request_response_invalid(dispatcher):
    topic = "v1/devices/me/attributes/response/bad"
    with pytest.raises(ValueError):
        dispatcher.parse_attribute_request_response(topic, b"invalid")


def test_parse_attribute_update(dispatcher):
    payload = dumps({"shared": {"humidity": 60}})
    with patch.object(AttributeUpdate, "_deserialize_from_dict", return_value="AU"):
        result = dispatcher.parse_attribute_update(payload)
        assert result == "AU"


def test_parse_attribute_update_invalid(dispatcher):
    with pytest.raises(ValueError):
        dispatcher.parse_attribute_update(b"{bad}")


def test_parse_rpc_request(dispatcher):
    topic = "v1/devices/me/rpc/request/123"
    payload = dumps({"params": {"a": 1}})
    with patch.object(RPCRequest, "_deserialize_from_dict", return_value="REQ"):
        assert dispatcher.parse_rpc_request(topic, payload) == "REQ"


def test_parse_rpc_request_invalid(dispatcher):
    topic = "v1/devices/me/rpc/request/NaN"
    with pytest.raises(ValueError):
        dispatcher.parse_rpc_request(topic, b"{}")


def test_parse_rpc_response(dispatcher):
    topic = "v1/devices/me/rpc/response/999"
    payload = dumps({"value": "done"})
    with patch.object(RPCResponse, "build", return_value="RSP"):
        assert dispatcher.parse_rpc_response(topic, payload) == "RSP"


def test_parse_rpc_response_with_error(dispatcher):
    topic = "v1/devices/me/rpc/response/888"
    error = ValueError("fail")
    with patch.object(RPCResponse, "build", return_value="ERR"):
        assert dispatcher.parse_rpc_response(topic, error) == "ERR"


def test_parse_rpc_response_invalid(dispatcher):
    topic = "v1/devices/me/rpc/response/NaN"
    with pytest.raises(ValueError):
        dispatcher.parse_rpc_response(topic, b"bad")


@pytest.mark.asyncio
async def test_build_uplink_payloads_empty(dispatcher: JsonMessageDispatcher):
    assert dispatcher.build_uplink_payloads([]) == []


@pytest.mark.asyncio
async def test_build_uplink_payloads_only_attributes(dispatcher: JsonMessageDispatcher):
    msg = build_msg(with_attr=True)
    with patch.object(dispatcher._splitter, "split_attributes", return_value=[msg]):
        result = dispatcher.build_uplink_payloads([msg])
        assert len(result) == 1
        topic, payload, count, futures = result[0]
        assert topic == DEVICE_ATTRIBUTES_TOPIC
        assert count == 1
        assert b"a" in payload


@pytest.mark.asyncio
async def test_build_uplink_payloads_only_timeseries(dispatcher: JsonMessageDispatcher):
    msg = build_msg(with_ts=True)
    with patch.object(dispatcher._splitter, "split_timeseries", return_value=[msg]):
        result = dispatcher.build_uplink_payloads([msg])
        assert len(result) == 1
        topic, payload, count, futures = result[0]
        assert topic == DEVICE_TELEMETRY_TOPIC
        assert count == 1
        assert b"ts" in payload


@pytest.mark.asyncio
async def test_build_uplink_payloads_both(dispatcher: JsonMessageDispatcher):
    msg = build_msg(with_attr=True, with_ts=True)
    with patch.object(dispatcher._splitter, "split_attributes", return_value=[msg]), \
         patch.object(dispatcher._splitter, "split_timeseries", return_value=[msg]):
        result = dispatcher.build_uplink_payloads([msg])
        assert len(result) == 2
        topics = {r[0] for r in result}
        assert DEVICE_ATTRIBUTES_TOPIC in topics
        assert DEVICE_TELEMETRY_TOPIC in topics


@pytest.mark.asyncio
async def test_build_uplink_payloads_multiple_devices(dispatcher: JsonMessageDispatcher):
    msg1 = build_msg(device="dev1", with_attr=True)
    msg2 = build_msg(device="dev2", with_ts=True)
    with patch.object(dispatcher._splitter, "split_attributes", side_effect=lambda x: x), \
         patch.object(dispatcher._splitter, "split_timeseries", side_effect=lambda x: x):
        result = dispatcher.build_uplink_payloads([msg1, msg2])
        topics = {r[0] for r in result}
        assert DEVICE_ATTRIBUTES_TOPIC in topics or DEVICE_TELEMETRY_TOPIC in topics


def test_build_payload_with_device_name(dispatcher: JsonMessageDispatcher):
    msg = build_msg(with_ts=True)
    payload = dispatcher.build_payload(msg, True)
    assert isinstance(payload, bytes)
    assert msg.device_name.encode() in payload


def test_build_payload_without_device_name(dispatcher: JsonMessageDispatcher):
    builder = DeviceUplinkMessageBuilder().add_attributes(AttributeEntry("x", 9))
    msg = builder.build()
    payload = dispatcher.build_payload(msg, False)
    assert isinstance(payload, bytes)
    assert b"x" in payload


def test_pack_attributes():
    builder = DeviceUplinkMessageBuilder().add_attributes(AttributeEntry("x", 10))
    msg = builder.build()
    result = JsonMessageDispatcher.pack_attributes(msg)
    assert isinstance(result, dict)
    assert "x" in result


def test_pack_timeseries_uses_now(monkeypatch):
    monkeypatch.setattr("tb_mqtt_client.service.message_dispatcher.datetime", MagicMock())
    ts_entry = TimeseriesEntry("temp", 23, ts=None)
    builder = DeviceUplinkMessageBuilder().add_timeseries(ts_entry)
    msg = builder.build()
    packed = JsonMessageDispatcher.pack_timeseries(msg)
    assert isinstance(packed, list)
    assert "ts" in packed[0]
    assert "values" in packed[0]


def test_build_uplink_payloads_error_handling(dispatcher: JsonMessageDispatcher):
    with patch("tb_mqtt_client.service.message_dispatcher.DeviceUplinkMessage.has_attributes", side_effect=Exception("boom")):
        msg = build_msg(with_attr=True)
        with pytest.raises(Exception, match="boom"):
            dispatcher.build_uplink_payloads([msg])


def test_parse_provisioning_response_success(dispatcher, dummy_provisioning_request):
    payload_dict = {"status": "SUCCESS", "credentialsType": "ACCESS_TOKEN"}
    payload_bytes = dumps(payload_dict)

    with patch.object(ProvisioningResponse, "build", return_value="SUCCESS_RESPONSE") as mock_build:
        result = dispatcher.parse_provisioning_response(dummy_provisioning_request, payload_bytes)
        assert result == "SUCCESS_RESPONSE"
        mock_build.assert_called_once_with(dummy_provisioning_request, payload_dict)


def test_parse_provisioning_response_failure(dispatcher, dummy_provisioning_request):
    broken_bytes = b"{not_json"

    with patch.object(ProvisioningResponse, "build", return_value="FAILURE_RESPONSE") as mock_build:
        result = dispatcher.parse_provisioning_response(dummy_provisioning_request, broken_bytes)
        assert result == "FAILURE_RESPONSE"
        mock_build.assert_called_once()
        args = mock_build.call_args[0]
        assert args[0] == dummy_provisioning_request
        assert args[1]["status"] == "FAILURE"
        assert "errorMsg" in args[1]