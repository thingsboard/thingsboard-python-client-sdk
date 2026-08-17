# Copyright 2026. ThingsBoard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest.mock import Mock, patch

from tb_device_mqtt import TBDeviceMqttClient
from tb_gateway_mqtt import TBGatewayMqttClient


class MqttProxyTest(unittest.TestCase):

    @patch('tb_device_mqtt.Thread.start')
    def test_device_client_proxy_set_delegates_to_paho_client(self, _):
        client = TBDeviceMqttClient('localhost', username='token')
        client._client.proxy_set = Mock(return_value=None)

        client.proxy_set(proxy_type='HTTP', proxy_addr='proxy.local', proxy_port=3128)

        client._client.proxy_set.assert_called_once_with(
            proxy_type='HTTP', proxy_addr='proxy.local', proxy_port=3128)

    @patch('tb_device_mqtt.Thread.start')
    def test_gateway_client_inherits_proxy_set(self, _):
        client = TBGatewayMqttClient('localhost', username='token')
        client._client.proxy_set = Mock(return_value=None)

        client.proxy_set(proxy_type='HTTP', proxy_addr='proxy.local', proxy_port=3128)

        client._client.proxy_set.assert_called_once_with(
            proxy_type='HTTP', proxy_addr='proxy.local', proxy_port=3128)


if __name__ == '__main__':
    unittest.main()
