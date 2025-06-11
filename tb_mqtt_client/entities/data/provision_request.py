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

class ProvisionRequest:
    def __init__(self, provision_device_key, provision_device_secret, device_name=None, gateway=None):
        self.provision_device_key = provision_device_key
        self.provision_device_secret = provision_device_secret
        self.device_name = device_name
        self.gateway = gateway

    def to_dict(self):
        provision_request = {
            "provisionDeviceKey": self.provision_device_key,
            "provisionDeviceSecret": self.provision_device_secret,
        }
        if self.device_name is not None:
            provision_request["deviceName"] = self.device_name
        if self.gateway is not None:
            provision_request["gateway"] = self.gateway

        return provision_request


class ProvisionRequestAccessToken(ProvisionRequest):
    def __init__(self, provision_device_key, provision_device_secret, access_token, device_name=None, gateway=None):
        super().__init__(provision_device_key, provision_device_secret, device_name, gateway)
        self.credentials_type = "ACCESS_TOKEN"
        self.access_token = access_token

    def to_dict(self):
        provision_request = super().to_dict()
        provision_request["token"] = self.access_token
        provision_request["credentialsType"] = "ACCESS_TOKEN"
        return provision_request


class ProvisionRequestBasic(ProvisionRequest):
    def __init__(self, provision_device_key, provision_device_secret,
                 client_id=None, username=None, password=None, device_name=None, gateway=None):
        super().__init__(provision_device_key, provision_device_secret, device_name, gateway)
        self.credentials_type = "MQTT_BASIC"
        self.client_id = client_id
        self.username = username
        self.password = password

    def to_dict(self):
        provision_request = super().to_dict()
        provision_request["credentialsType"] = "MQTT_BASIC"
        provision_request["username"] = self.username
        provision_request["password"] = self.password
        provision_request["clientId"] = self.client_id
        return provision_request


class ProvisionRequestX509(ProvisionRequest):
    def __init__(self, provision_device_key, provision_device_secret, hash, device_name=None, gateway=None):
        super().__init__(provision_device_key, provision_device_secret, device_name, gateway)
        self.credentials_type = "X509_CERTIFICATE"
        self.hash = hash

    def to_dict(self):
        provision_request = super().to_dict()
        provision_request["credentialsType"] = "X509_CERTIFICATE"
        provision_request["hash"] = self.hash
        return provision_request
