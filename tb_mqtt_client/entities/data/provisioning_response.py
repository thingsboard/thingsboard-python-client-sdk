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

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, ProvisioningCredentialsType


class ProvisioningResponseStatus(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "FAILURE"

    def __str__(self):
        return self.value


@dataclass(frozen=True)
class ProvisioningResponse:
    status: ProvisioningResponseStatus
    result: Optional[dict] = None
    error: Optional[str] = None

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of ProvisioningResponse is not allowed. Use ProvisioningResponse.build(result, error).")  # noqa

    def __repr__(self) -> str:
        return f"ProvisioningResponse(status={self.status}, result={self.result}, error={self.error})"

    @classmethod
    def build(cls, provision_request: 'ProvisioningRequest', payload: dict) -> 'ProvisioningResponse':
        """
        Constructs a ProvisioningResponse explicitly.
        """
        self = object.__new__(cls)

        if payload.get('status') == ProvisioningResponseStatus.ERROR.value:
            object.__setattr__(self, 'error', payload.get('errorMsg'))
            object.__setattr__(self, 'status', ProvisioningResponseStatus.ERROR)
            object.__setattr__(self, 'result', None)
        else:
            device_config = ProvisioningResponse._build_device_config(provision_request, payload)

            object.__setattr__(self, 'result', device_config)
            object.__setattr__(self, 'status', ProvisioningResponseStatus.SUCCESS)
            object.__setattr__(self, 'error', None)

        return self

    @staticmethod
    def _build_device_config(provision_request: 'ProvisioningRequest', payload: dict):
        device_config = DeviceConfig()
        device_config.host = provision_request.host
        device_config.port = provision_request.port

        if provision_request.credentials.credentials_type is None or \
                provision_request.credentials.credentials_type == ProvisioningCredentialsType.ACCESS_TOKEN:
            device_config.access_token = payload['credentialsValue']
        elif provision_request.credentials.credentials_type == ProvisioningCredentialsType.MQTT_BASIC:
            device_config.client_id = payload['credentialsValue']['clientId']
            device_config.username = payload['credentialsValue']['userName']
            device_config.password = payload['credentialsValue']['password']
        elif provision_request.credentials.credentials_type == ProvisioningCredentialsType.X509_CERTIFICATE:
            device_config.ca_cert = provision_request.credentials.ca_cert_path
            device_config.client_cert = provision_request.credentials.public_cert_path
            device_config.private_key = provision_request.credentials.private_key_path

        return device_config
