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

import os
from typing import Optional, Dict, Any


class DeviceConfig:
    def __init__(self):
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

    def use_tls_auth(self) -> bool:
        return all([self.ca_cert, self.client_cert, self.private_key])

    def use_tls(self) -> bool:
        return self.ca_cert is not None

    def __repr__(self):
        return (f"<DeviceConfig host={self.host} port={self.port} "
                f"auth={'token' if self.access_token else 'user/pass'} "
                f"tls_auth={self.use_tls_auth()} "
                f"tls={self.use_tls()}>")


class GatewayConfig(DeviceConfig):
    def __init__(self):
        super().__init__()

        # Gateway-specific options
        self.gateway_name: Optional[str] = os.getenv("TB_GATEWAY_NAME")

        # Rate limits for devices connected through the gateway
        self.device_messages_rate_limit: Optional[str] = os.getenv("TB_DEVICE_MESSAGES_RATE_LIMIT")
        self.device_telemetry_rate_limit: Optional[str] = os.getenv("TB_DEVICE_TELEMETRY_RATE_LIMIT")
        self.device_telemetry_dp_rate_limit: Optional[str] = os.getenv("TB_DEVICE_TELEMETRY_DP_RATE_LIMIT")

        # Default device type for auto-registered devices
        self.default_device_type: Optional[str] = os.getenv("TB_DEFAULT_DEVICE_TYPE", "default")

        # Whether to automatically register new devices
        self.auto_register_devices: bool = os.getenv("TB_AUTO_REGISTER_DEVICES", "true").lower() == "true"

    def __repr__(self):
        return (f"<GatewayConfig host={self.host} port={self.port} "
                f"auth={'token' if self.access_token else 'user/pass'} "
                f"gateway_name={self.gateway_name} "
                f"auto_register={self.auto_register_devices} "
                f"tls={self.use_tls()}>")
