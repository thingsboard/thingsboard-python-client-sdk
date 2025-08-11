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
from unittest.mock import patch, mock_open

import pytest

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.constants.provisioning import ProvisioningResponseStatus
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, AccessTokenProvisioningCredentials, \
    BasicProvisioningCredentials, X509ProvisioningCredentials
from tb_mqtt_client.entities.data.provisioning_response import ProvisioningResponse


@pytest.fixture
def access_token_request():
    return ProvisioningRequest(
        host="my-host",
        port=1883,
        credentials=AccessTokenProvisioningCredentials(
            provision_device_key="provision_device_key",
            provision_device_secret="provision_device_secret",
            access_token="access-token123"
        )
    )


@pytest.fixture
def mqtt_basic_request():
    return ProvisioningRequest(
        host="test-host",
        port=8883,
        credentials=BasicProvisioningCredentials(
            provision_device_key="provision_device_key",
            provision_device_secret="provision_device_secret",
            client_id="my-client-id",
            username="user1",
            password="pass123"
        )
    )


@pytest.fixture
def x509_request():
    mocked_cert_content = "-----BEGIN CERTIFICATE-----\nABCDEF\n-----END CERTIFICATE-----"

    with patch("builtins.open", mock_open(read_data=mocked_cert_content)):
        return ProvisioningRequest(
            host="secure-host",
            port=8883,
            credentials=X509ProvisioningCredentials(
                provision_device_key="provision_device_key",
                provision_device_secret="provision_device_secret",
                private_key_path="/path/to/private.key",
                public_cert_path="/path/to/client.crt",
                ca_cert_path="/path/to/ca.crt"
            )
        )


def test_direct_instantiation_fails():
    with pytest.raises(TypeError, match="Direct instantiation of ProvisioningResponse is not allowed"):
        ProvisioningResponse(status=ProvisioningResponseStatus.SUCCESS)


def test_error_response():
    access_token_credentials = AccessTokenProvisioningCredentials("provision_device_key", "provision_device_secret")
    request = ProvisioningRequest(host="any", port=1883, credentials=access_token_credentials)
    payload = {"errorMsg": "Provisioning failed", "status": ProvisioningResponseStatus.ERROR.value}

    response = ProvisioningResponse.build(request, payload)

    assert response.status == ProvisioningResponseStatus.ERROR
    assert response.result is None
    assert response.error == "Provisioning failed"


def test_success_access_token(access_token_request):
    payload = {"credentialsValue": "ACCESS-TOKEN-123", "status": "SUCCESS"}

    response = ProvisioningResponse.build(access_token_request, payload)

    assert response.status == ProvisioningResponseStatus.SUCCESS
    assert response.error is None
    assert isinstance(response.result, DeviceConfig)
    assert response.result.access_token == "ACCESS-TOKEN-123"
    assert response.result.host == "my-host"
    assert response.result.port == 1883


def test_success_mqtt_basic(mqtt_basic_request):
    payload = {
        "credentialsValue": {
            "clientId": "my-client-id",
            "userName": "user1",
            "password": "pass123"
        },
        "status": "SUCCESS"
    }

    response = ProvisioningResponse.build(mqtt_basic_request, payload)

    assert response.status == ProvisioningResponseStatus.SUCCESS
    assert isinstance(response.result, DeviceConfig)
    config = response.result
    assert config.client_id == "my-client-id"
    assert config.username == "user1"
    assert config.password == "pass123"
    assert config.host == "test-host"
    assert config.port == 8883
    assert response.error is None


def test_success_x509(x509_request):
    payload = {"credentialsValue": None, "status": "SUCCESS"}  # Should be ignored for X509

    response = ProvisioningResponse.build(x509_request, payload)

    assert response.status == ProvisioningResponseStatus.SUCCESS
    config = response.result
    assert config.ca_cert == "/path/to/ca.crt"
    assert config.client_cert == "/path/to/client.crt"
    assert config.private_key == "/path/to/private.key"
    assert config.host == "secure-host"
    assert config.port == 8883
    assert response.error is None


def test_repr_output(access_token_request):
    payload = {"credentialsValue": "access-token", "status": "SUCCESS"}
    response = ProvisioningResponse.build(access_token_request, payload)

    r = repr(response)
    assert r.startswith("ProvisioningResponse(status=SUCCESS")
    assert "DeviceConfig(" in r
    assert "host=my-host" in r
    assert "port=1883" in r
    assert "error=None" in r


def test_missing_credentials_value_for_access_token(access_token_request):
    payload = {"status": "SUCCESS"}  # Missing 'credentialsValue'

    with pytest.raises(KeyError):
        ProvisioningResponse.build(access_token_request, payload)


def test_mqtt_basic_missing_fields(mqtt_basic_request):
    payload = {"credentialsValue": {}, "status": "SUCCESS"}  # All fields missing

    with pytest.raises(KeyError):
        ProvisioningResponse.build(mqtt_basic_request, payload)


def test_access_token_none_is_accepted(access_token_request):
    payload = {"credentialsValue": None, "status": "SUCCESS"}
    response = ProvisioningResponse.build(access_token_request, payload)

    assert response.status == ProvisioningResponseStatus.SUCCESS
    assert response.result.access_token is None
