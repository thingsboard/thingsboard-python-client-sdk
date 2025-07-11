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

import os
from typing import Optional


class DeviceConfig:
    """
    Configuration class for ThingsBoard device clients.
    This class loads configuration options from environment variables, allowing for flexible deployment
    and easy customization of device connection settings.
    """
    def __init__(self, config = None):
        if config is not None:
            self.host: str = config.get("host", "localhost")
            self.port: int = config.get("port", 1883)
            self.access_token: Optional[str] = config.get("access_token")
            self.username: Optional[str] = config.get("username")
            self.password: Optional[str] = config.get("password")
            self.client_id: Optional[str] = config.get("client_id")
            self.ca_cert: Optional[str] = config.get("ca_cert")
            self.client_cert: Optional[str] = config.get("client_cert")
            self.private_key: Optional[str] = config.get("private_key")

        self.host: str = os.getenv("TB_HOST")
        self.port: int = int(os.getenv("TB_PORT", 1883))

        # Authentication options
        self.access_token: Optional[str] = os.getenv("TB_ACCESS_TOKEN")
        self.username: Optional[str] = os.getenv("TB_USERNAME")
        self.password: Optional[str] = os.getenv("TB_PASSWORD")

        # Optional
        self.client_id: Optional[str] = os.getenv("TB_CLIENT_ID")

        # TLS options
        self.ca_cert: Optional[str] = os.getenv("TB_CA_CERT")
        self.client_cert: Optional[str] = os.getenv("TB_CLIENT_CERT")
        self.private_key: Optional[str] = os.getenv("TB_PRIVATE_KEY")

        # Default values
        self.qos: int = int(os.getenv("TB_QOS", 1))

    def use_tls_auth(self) -> bool:
        return all([self.ca_cert, self.client_cert, self.private_key])

    def use_tls(self) -> bool:
        return self.ca_cert is not None

    def __repr__(self):
        return (f"DeviceConfig(host={self.host}, port={self.port}, "
                f"auth={'token' if self.access_token else 'user/pass'} "
                f"client_id={self.client_id} "
                f"tls={self.use_tls()})")


class GatewayConfig(DeviceConfig):
    """
    Configuration class for ThingsBoard gateway clients.
    This class extends DeviceConfig to include additional options specific to gateways.
    """
    def __init__(self, config=None):
        # TODO: REFACTOR, temporary solution for development
        super().__init__(config)

        if os.getenv("TB_GW_HOST") is not None:
            self.host: str = os.getenv("TB_GW_HOST")
        if os.getenv("TB_GW_PORT") is not None:
            self.port: int = int(os.getenv("TB_GW_PORT", 1883))

        if os.getenv("TB_GW_ACCESS_TOKEN") is not None:
            self.access_token: Optional[str] = os.getenv("TB_GW_ACCESS_TOKEN")
        if os.getenv("TB_GW_USERNAME") is not None:
            self.username: Optional[str] = os.getenv("TB_GW_USERNAME")
        if os.getenv("TB_GW_PASSWORD") is not None:
            self.password: Optional[str] = os.getenv("TB_GW_PASSWORD")

        if os.getenv("TB_GW_CLIENT_ID") is not None:
            self.client_id: Optional[str] = os.getenv("TB_GW_CLIENT_ID")

        if os.getenv("TB_GW_CA_CERT") is not None:
            self.ca_cert: Optional[str] = os.getenv("TB_GW_CA_CERT")
        if os.getenv("TB_GW_CLIENT_CERT") is not None:
            self.client_cert: Optional[str] = os.getenv("TB_GW_CLIENT_CERT")
        if os.getenv("TB_GW_PRIVATE_KEY") is not None:
            self.private_key: Optional[str] = os.getenv("TB_GW_PRIVATE_KEY")

        if os.getenv("TB_GW_QOS") is not None:
            self.qos: int = int(os.getenv("TB_GW_QOS", 1))

    def __repr__(self):
        return (f"GatewayConfig(host={self.host}, port={self.port}, "
                f"auth={'token' if self.access_token else 'user/pass'} "
                f"client_id={self.client_id} "
                f"tls={self.use_tls()})")
