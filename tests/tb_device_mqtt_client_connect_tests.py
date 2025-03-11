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
from unittest.mock import patch, MagicMock, call
from threading import Thread
from tb_device_mqtt import TBDeviceMqttClient, TBTimeoutException
from paho.mqtt.client import ReasonCodes


class TestTBDeviceMqttClientOnConnect(unittest.TestCase):
    def test_on_connect_success(self):
        client = TBDeviceMqttClient("host", 1883, "username")
        client._subscribe_to_topic = MagicMock()

        client._on_connect(client=None, userdata=None, flags=None, result_code=0)

        self.assertTrue(client.is_connected())

        expected_sub_calls = [
            call('v1/devices/me/attributes', qos=client.quality_of_service),
            call('v1/devices/me/attributes/response/+', qos=client.quality_of_service),
            call('v1/devices/me/rpc/request/+', qos=client.quality_of_service),
            call('v1/devices/me/rpc/response/+', qos=client.quality_of_service),
        ]
        client._subscribe_to_topic.assert_has_calls(expected_sub_calls, any_order=False)

        self.assertTrue(client._TBDeviceMqttClient__request_service_configuration_required)

    def test_on_connect_fail_known_code(self):
        client = TBDeviceMqttClient("host", 1883, "username")
        client._subscribe_to_topic = MagicMock()

        known_error_code = 1
        client._on_connect(client=None, userdata=None, flags=None, result_code=known_error_code)

        self.assertFalse(client.is_connected())
        client._subscribe_to_topic.assert_not_called()

    def test_on_connect_fail_unknown_code(self):
        client = TBDeviceMqttClient("host", 1883, "username")
        client._subscribe_to_topic = MagicMock()

        client._on_connect(client=None, userdata=None, flags=None, result_code=999)

        self.assertFalse(client.is_connected())
        client._subscribe_to_topic.assert_not_called()

    def test_on_connect_fail_reasoncodes(self):
        client = TBDeviceMqttClient("host", 1883, "username")
        client._subscribe_to_topic = MagicMock()

        mock_rc = MagicMock(spec=ReasonCodes)
        mock_rc.getName.return_value = "SomeError"

        client._on_connect(client=None, userdata=None, flags=None, result_code=mock_rc)

        self.assertFalse(client.is_connected())
        client._subscribe_to_topic.assert_not_called()

    def test_on_connect_callback_with_tb_client(self):
        client = TBDeviceMqttClient("host", 1883, "username")

        def my_connect_callback(client_param, userdata, flags, rc, *args, tb_client=None):
            self.assertIsNotNone(tb_client, "tb_client must be passed to the callback")
            self.assertEqual(tb_client, client)

        client._TBDeviceMqttClient__connect_callback = my_connect_callback

        client._on_connect(client=None, userdata="test_user_data", flags="test_flags", result_code=0)

    def test_on_connect_callback_without_tb_client(self):
        client = TBDeviceMqttClient("host", 1883, "username")

        def my_callback(client_param, userdata, flags, rc, *args):
            pass

        client._TBDeviceMqttClient__connect_callback = my_callback

        client._on_connect(client=None, userdata="test_user_data", flags="test_flags", result_code=0)


class TestTBDeviceMqttClient(unittest.TestCase):
    @patch('tb_device_mqtt.paho.Client')
    def setUp(self, mock_paho_client):
        self.mock_mqtt_client = mock_paho_client.return_value
        self.client = TBDeviceMqttClient(
            host='host',
            port=1883,
            username='username',
            password=None
        )
        self.client._TBDeviceMqttClient__service_loop = Thread(target=lambda: None)
        self.client._TBDeviceMqttClient__updating_thread = Thread(target=lambda: None)

        if not hasattr(self.client, '_client'):
            self.client._client = self.mock_mqtt_client

    def test_connect(self):
        self.client.connect()
        self.mock_mqtt_client.connect.assert_called_with('host', 1883, keepalive=120)
        self.mock_mqtt_client.loop_start.assert_called()

    def test_disconnect(self):
        self.client.disconnect()
        self.mock_mqtt_client.disconnect.assert_called()
        self.mock_mqtt_client.loop_stop.assert_called()

    def test_send_telemetry(self):
        self.client._publish_data = MagicMock()
        telemetry = {'temp': 22}
        self.client.send_telemetry(telemetry)
        self.client._publish_data.assert_called_with([telemetry], 'v1/devices/me/telemetry', 1, True)

    def test_timeout_exception(self):
        with self.assertRaises(TBTimeoutException):
            raise TBTimeoutException("Timeout occurred")

    def test_thread_attributes(self):
        self.assertTrue(isinstance(self.client._TBDeviceMqttClient__service_loop, Thread))
        self.assertTrue(isinstance(self.client._TBDeviceMqttClient__updating_thread, Thread))
