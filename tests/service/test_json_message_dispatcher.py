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
from unittest.mock import MagicMock, patch, mock_open
from orjson import dumps

from tb_mqtt_client.service.message_dispatcher import JsonMessageDispatcher
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, ProvisioningCredentialsType, \
    BasicProvisioningCredentials, X509ProvisioningCredentials, AccessTokenProvisioningCredentials


class DummyClaimRequest:
    def __init__(self, secret_key="key"):
        self.secret_key = secret_key

    def to_payload_format(self):
        return {"secretKey": self.secret_key}


class DummyProvisioningRequest:
    def __init__(self):
        self.device_name = "dev"
        self.gateway = True
        self.credentials = MagicMock()
        self.credentials.provision_device_key = "key"
        self.credentials.provision_device_secret = "secret"
        self.credentials.credentials_type = ProvisioningCredentialsType.ACCESS_TOKEN
        self.credentials.access_token = "token"


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
    req = DummyClaimRequest()
    topic, payload = dispatcher.build_claim_request(req)
    assert topic == mqtt_topics.DEVICE_CLAIM_TOPIC
    assert b"secretKey" in payload


def test_build_claim_request_invalid(dispatcher):
    req = DummyClaimRequest(secret_key=None)  # Simulating an invalid request # noqa
    with pytest.raises(ValueError):
        dispatcher.build_claim_request(req)


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


if __name__ == '__main__':
    pytest.main([__file__])
