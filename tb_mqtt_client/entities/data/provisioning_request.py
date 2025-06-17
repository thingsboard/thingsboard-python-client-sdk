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

from enum import Enum
from typing import Optional


class ProvisioningRequest:
    def __init__(self, host, credentials: 'ProvisioningCredentials', port: str = "1883",
                 device_name: Optional[str] = None, gateway: Optional[bool] = False):
        self.host = host
        self.port = port
        self.credentials = credentials
        self.device_name = device_name
        self.gateway = gateway


class ProvisioningCredentialsType(Enum):
    ACCESS_TOKEN = "ACCESS_TOKEN"
    MQTT_BASIC = "MQTT_BASIC"
    X509_CERTIFICATE = "X509_CERTIFICATE"


class ProvisioningCredentials:
    def __init__(self, provision_device_key: str, provision_device_secret: str):
        self.provision_device_key = provision_device_key
        self.provision_device_secret = provision_device_secret
        self.credentials_type = None


class AccessTokenProvisioningCredentials(ProvisioningCredentials):
    def __init__(self, provision_device_key: str, provision_device_secret: str, access_token: str):
        super().__init__(provision_device_key, provision_device_secret)
        self.access_token = access_token
        self.credentials_type = ProvisioningCredentialsType.ACCESS_TOKEN


class BasicProvisioningCredentials(ProvisioningCredentials):
    def __init__(self, provision_device_key, provision_device_secret,
                 client_id: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None):
        super().__init__(provision_device_key, provision_device_secret)
        self.client_id = client_id
        self.username = username
        self.password = password
        self.credentials_type = ProvisioningCredentialsType.MQTT_BASIC


class X509ProvisioningCredentials(ProvisioningCredentials):
    def __init__(self, provision_device_key, provision_device_secret,
                 private_key_path: str, public_cert_path: str, ca_cert_path: str):
        super().__init__(provision_device_key, provision_device_secret)
        self.private_key_path = private_key_path
        self.ca_cert_path = ca_cert_path
        self.public_cert_path = public_cert_path
        self.public_cert = self._load_public_cert_path(public_cert_path)
        self.credentials_type = ProvisioningCredentialsType.X509_CERTIFICATE

    def _load_public_cert_path(public_cert_path):
        content = ''

        try:
            with open(public_cert_path, 'r') as file:
                content = file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Public certificate file not found: {public_cert_path}")
        except IOError as e:
            raise IOError(f"Error reading public certificate file {public_cert_path}: {e}")

        return content.strip() if content else None
