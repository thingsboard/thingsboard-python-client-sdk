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
import unittest

from tb_mqtt_client.common.config_loader import DeviceConfig, GatewayConfig


class TestDeviceConfig(unittest.TestCase):

    def loads_default_values_when_env_vars_missing(self):
        os.environ.clear()
        config = DeviceConfig()
        self.assertEqual(config.host, None)
        self.assertEqual(config.port, 1883)
        self.assertEqual(config.access_token, None)
        self.assertEqual(config.username, None)
        self.assertEqual(config.password, None)
        self.assertEqual(config.client_id, None)
        self.assertEqual(config.ca_cert, None)
        self.assertEqual(config.client_cert, None)
        self.assertEqual(config.private_key, None)
        self.assertEqual(config.qos, 1)

    def loads_values_from_env_vars(self):
        os.environ["TB_HOST"] = "test_host"
        os.environ["TB_PORT"] = "8883"
        os.environ["TB_ACCESS_TOKEN"] = "test_token"
        os.environ["TB_USERNAME"] = "test_user"
        os.environ["TB_PASSWORD"] = "test_pass"
        os.environ["TB_CLIENT_ID"] = "test_client"
        os.environ["TB_CA_CERT"] = "test_ca"
        os.environ["TB_CLIENT_CERT"] = "test_cert"
        os.environ["TB_PRIVATE_KEY"] = "test_key"
        os.environ["TB_QOS"] = "2"

        config = DeviceConfig()
        self.assertEqual(config.host, "test_host")
        self.assertEqual(config.port, 8883)
        self.assertEqual(config.access_token, "test_token")
        self.assertEqual(config.username, "test_user")
        self.assertEqual(config.password, "test_pass")
        self.assertEqual(config.client_id, "test_client")
        self.assertEqual(config.ca_cert, "test_ca")
        self.assertEqual(config.client_cert, "test_cert")
        self.assertEqual(config.private_key, "test_key")
        self.assertEqual(config.qos, 2)

    def detects_tls_auth_correctly(self):
        os.environ["TB_CA_CERT"] = "test_ca"
        os.environ["TB_CLIENT_CERT"] = "test_cert"
        os.environ["TB_PRIVATE_KEY"] = "test_key"
        config = DeviceConfig()
        self.assertTrue(config.use_tls_auth())

    def detects_tls_correctly(self):
        os.environ["TB_CA_CERT"] = "test_ca"
        config = DeviceConfig()
        self.assertTrue(config.use_tls())


class TestGatewayConfig(unittest.TestCase):

    def loads_gateway_specific_env_vars(self):
        os.environ["TB_GW_HOST"] = "gw_host"
        os.environ["TB_GW_PORT"] = "8884"
        os.environ["TB_GW_ACCESS_TOKEN"] = "gw_token"
        os.environ["TB_GW_USERNAME"] = "gw_user"
        os.environ["TB_GW_PASSWORD"] = "gw_pass"
        os.environ["TB_GW_CLIENT_ID"] = "gw_client"
        os.environ["TB_GW_CA_CERT"] = "gw_ca"
        os.environ["TB_GW_CLIENT_CERT"] = "gw_cert"
        os.environ["TB_GW_PRIVATE_KEY"] = "gw_key"
        os.environ["TB_GW_QOS"] = "0"

        config = GatewayConfig()
        self.assertEqual(config.host, "gw_host")
        self.assertEqual(config.port, 8884)
        self.assertEqual(config.access_token, "gw_token")
        self.assertEqual(config.username, "gw_user")
        self.assertEqual(config.password, "gw_pass")
        self.assertEqual(config.client_id, "gw_client")
        self.assertEqual(config.ca_cert, "gw_ca")
        self.assertEqual(config.client_cert, "gw_cert")
        self.assertEqual(config.private_key, "gw_key")
        self.assertEqual(config.qos, 0)

    def falls_back_to_device_config_when_gateway_env_vars_missing(self):
        os.environ.clear()
        os.environ["TB_HOST"] = "device_host"
        os.environ["TB_PORT"] = "1884"
        config = GatewayConfig()
        self.assertEqual(config.host, "device_host")
        self.assertEqual(config.port, 1884)
