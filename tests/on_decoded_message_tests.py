# Copyright 2025. ThingsBoard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import threading
from unittest.mock import MagicMock
from tb_gateway_mqtt import (
    TBGatewayMqttClient,
    GATEWAY_ATTRIBUTES_RESPONSE_TOPIC,
    GATEWAY_ATTRIBUTES_TOPIC,
    GATEWAY_RPC_TOPIC
)


class FakeMessage:
    def __init__(self, topic):
        self.topic = topic


class TestOnDecodedMessage(unittest.TestCase):
    def setUp(self):
        self.client = TBGatewayMqttClient("localhost", 1883, "dummy_token")
        if not hasattr(self.client, "_lock"):
            self.client._lock = threading.Lock()

    def test_on_decoded_message_attributes_response_non_tuple(self):
        content = {"id": 123, "data": "dummy_response"}
        fake_message = FakeMessage(topic=GATEWAY_ATTRIBUTES_RESPONSE_TOPIC)

        self.called = False

        def callback(msg, error):
            self.called = True
            self.callback_args = (msg, error)

        self.client._attr_request_dict = {123: callback}
        self.client._devices_connected_through_gateway_messages_rate_limit = MagicMock()

        self.client._on_decoded_message(content, fake_message)

        self.assertTrue(self.called)
        self.assertEqual(self.callback_args, (content, None))
        self.assertNotIn(123, self.client._attr_request_dict)
        self.client._devices_connected_through_gateway_messages_rate_limit.increase_rate_limit_counter.assert_called_with(
            1)

    def test_on_decoded_message_attributes_response_tuple(self):
        content = {"id": 456, "data": "dummy_response"}
        fake_message = FakeMessage(topic=GATEWAY_ATTRIBUTES_RESPONSE_TOPIC)

        self.called = False

        def callback(msg, error, extra):
            self.called = True
            self.callback_args = (msg, error, extra)

        self.client._attr_request_dict = {456: (callback, "extra_value")}
        self.client._devices_connected_through_gateway_messages_rate_limit = MagicMock()

        self.client._on_decoded_message(content, fake_message)

        self.assertTrue(self.called)
        self.assertEqual(self.callback_args, (content, None, "extra_value"))
        self.assertNotIn(456, self.client._attr_request_dict)
        self.client._devices_connected_through_gateway_messages_rate_limit.increase_rate_limit_counter.assert_called_with(
            1)

    def test_on_decoded_message_attributes_topic(self):
        content = {
            "device": "device1",
            "data": {"attr1": "value1", "attr2": "value2"}
        }
        fake_message = FakeMessage(topic=GATEWAY_ATTRIBUTES_TOPIC)

        self.flags = {"global": False, "device_all": False, "attr1": False, "attr2": False}

        def callback_global(msg):
            self.flags["global"] = True

        def callback_device_all(msg):
            self.flags["device_all"] = True

        def callback_attr1(msg):
            self.flags["attr1"] = True

        def callback_attr2(msg):
            self.flags["attr2"] = True

        self.client._TBGatewayMqttClient__sub_dict = {
            "*|*": {"global": callback_global},
            "device1|*": {"device_all": callback_device_all},
            "device1|attr1": {"attr1": callback_attr1},
            "device1|attr2": {"attr2": callback_attr2}
        }
        self.client._devices_connected_through_gateway_messages_rate_limit = MagicMock()

        self.client._on_decoded_message(content, fake_message)

        self.assertTrue(self.flags["global"])
        self.assertTrue(self.flags["device_all"])
        self.assertTrue(self.flags["attr1"])
        self.assertTrue(self.flags["attr2"])
        self.client._devices_connected_through_gateway_messages_rate_limit.increase_rate_limit_counter.assert_called_with(
            1)

    def test_on_decoded_message_rpc_topic(self):
        content = {"data": "dummy_rpc"}
        fake_message = FakeMessage(topic=GATEWAY_RPC_TOPIC)

        self.client._devices_connected_through_gateway_messages_rate_limit = MagicMock()
        self.called = False

        def rpc_handler(client, msg):
            self.called = True
            self.rpc_args = (client, msg)

        self.client.devices_server_side_rpc_request_handler = rpc_handler

        self.client._on_decoded_message(content, fake_message)

        self.assertTrue(self.called)
        self.assertEqual(self.rpc_args, (self.client, content))
        self.client._devices_connected_through_gateway_messages_rate_limit.increase_rate_limit_counter.assert_called_with(
            1)


if __name__ == '__main__':
    unittest.main()
