# Copyright 2024. ThingsBoard
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
from time import sleep, time

from tb_gateway_mqtt import TBGatewayMqttClient


class TBGatewayMqttClientTests(unittest.TestCase):
    """
    Before running tests, do the next steps:
    1. Create device "Example Name" in ThingsBoard
    2. Add shared attribute "attr" with value "hello" to created device
    """

    client = None

    device_name = 'Example Name'
    shared_attr_name = 'attr'
    shared_attr_value = 'hello'

    request_attributes_result = None
    subscribe_to_attribute = None
    subscribe_to_attribute_all = None
    subscribe_to_device_attribute_all = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TBGatewayMqttClient('127.0.0.1', 1883, 'TEST_GATEWAY_TOKEN')
        cls.client.connect(timeout=1)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.disconnect()

    @staticmethod
    def request_attributes_callback(result, exception=None):
        if exception is not None:
            TBGatewayMqttClientTests.request_attributes_result = exception
        else:
            TBGatewayMqttClientTests.request_attributes_result = result

    @staticmethod
    def callback(result):
        TBGatewayMqttClientTests.subscribe_to_device_attribute_all = result

    @staticmethod
    def callback_for_everything(result):
        TBGatewayMqttClientTests.subscribe_to_attribute_all = result

    @staticmethod
    def callback_for_specific_attr(result):
        TBGatewayMqttClientTests.subscribe_to_attribute = result

    def test_connect_disconnect_device(self):
        self.assertEqual(self.client.gw_connect_device(self.device_name).rc, 0)
        self.assertEqual(self.client.gw_disconnect_device(self.device_name).rc, 0)

    def test_request_attributes(self):
        self.client.gw_request_shared_attributes(self.device_name, [self.shared_attr_name],
                                                 self.request_attributes_callback)
        sleep(3)
        self.assertEqual(self.request_attributes_result,
                         {'id': 1, 'device': self.device_name, 'value': self.shared_attr_value})

    def test_send_telemetry_and_attributes(self):
        attributes = {"atr1": 1, "atr2": True, "atr3": "value3"}
        telemetry = {"ts": int(round(time() * 1000)), "values": {"key1": "11"}}
        self.assertEqual(self.client.gw_send_attributes(self.device_name, attributes).get(), 0)
        self.assertEqual(self.client.gw_send_telemetry(self.device_name, telemetry).get(), 0)

    def test_subscribe_to_attributes(self):
        self.client.gw_connect_device(self.device_name)

        self.client.gw_subscribe_to_all_attributes(self.callback_for_everything)
        self.client.gw_subscribe_to_attribute(self.device_name, self.shared_attr_name, self.callback_for_specific_attr)
        sub_id = self.client.gw_subscribe_to_all_device_attributes(self.device_name, self.callback)

        sleep(1)
        value = input("Updated attribute value: ")

        self.assertEqual(self.subscribe_to_attribute,
                         {'device': self.device_name, 'data': {self.shared_attr_name: value}})
        self.assertEqual(self.subscribe_to_attribute_all,
                         {'device': self.device_name, 'data': {self.shared_attr_name: value}})
        self.assertEqual(self.subscribe_to_device_attribute_all,
                         {'device': self.device_name, 'data': {self.shared_attr_name: value}})

        self.client.gw_unsubscribe(sub_id)


if __name__ == '__main__':
    unittest.main('tb_gateway_mqtt_client_tests')
