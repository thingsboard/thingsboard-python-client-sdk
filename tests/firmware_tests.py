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
from tb_device_mqtt import (
    TBDeviceMqttClient,
    RESULT_CODES
)
from paho.mqtt.client import ReasonCodes

class TestTBDeviceMqttClientOnConnect(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient("thingsboard_host", 1883, "dummy_token")
        self.client._subscribe_to_topic = MagicMock()
        self.client._TBDeviceMqttClient__connect_callback = lambda *args, **kwargs: None

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_known_code(self, mock_logger):
        self.client._on_connect(client=None, userdata=None, flags=None, result_code=1)
        self.assertFalse(self.client._TBDeviceMqttClient__is_connected)
        self.assertEqual(mock_logger.error.call_count, 1)

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_unknown_code(self, mock_logger):
        self.client._on_connect(client=None, userdata=None, flags=None, result_code=999)
        self.assertFalse(self.client._TBDeviceMqttClient__is_connected)
        self.assertEqual(mock_logger.error.call_count, 1)

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_reasoncodes(self, mock_logger):
        self.client = TBDeviceMqttClient("thingsboard_host", 1883, "dummy_token")
        mock_rc = MagicMock(spec=ReasonCodes)
        mock_rc.getName.return_value = "SomeError"
        self.client._on_connect(client=None, userdata=None, flags=None, result_code=mock_rc)
        self.assertFalse(self.client._TBDeviceMqttClient__is_connected)
        self.assertEqual(mock_logger.error.call_count, 1)

    @patch('tb_device_mqtt.log')
    def test_on_connect_callback_with_tb_client(self, mock_logger):
        self.client = TBDeviceMqttClient("thingsboard_host", 1883, "dummy_token")
        def my_connect_callback(client_param, userdata, flags, rc, *args, tb_client=None):
            self.assertIsNotNone(tb_client, "tb_client should be passed to the callback")
            self.assertEqual(tb_client, self.client)
        self.client._TBDeviceMqttClient__connect_callback = my_connect_callback
        self.client._on_connect(client=None, userdata="test_user_data", flags="test_flags", result_code=0)
        self.assertEqual(mock_logger.error.call_count, 0)

    @patch('tb_device_mqtt.log')
    def test_on_connect_callback_without_tb_client(self, mock_logger):
        self.client = TBDeviceMqttClient("thingsboard_host", 1883, "dummy_token")
        def my_callback(client_param, userdata, flags, rc, *args):
            self.assertTrue(True)
        self.client._TBDeviceMqttClient__connect_callback = my_callback
        self.client._on_connect(client=None, userdata="test_user_data", flags="test_flags", result_code=0)
        self.assertEqual(mock_logger.error.call_count, 0)

    def test_thread_attributes(self):
        self.assertTrue(isinstance(self.client._TBDeviceMqttClient__service_loop, Thread))
        self.assertTrue(isinstance(self.client._TBDeviceMqttClient__timeout_thread, Thread))


class TestTBDeviceMqttClientGeneral(unittest.TestCase):
    @patch('tb_device_mqtt.paho.Client')
    def setUp(self, mock_paho_client):
        self.mock_mqtt_client = mock_paho_client.return_value
        self.client = TBDeviceMqttClient(
            host='thingsboard_host',
            port=1883,
            username='dummy_token',
            password=None
        )
        self.client.firmware_info = {"fw_title": "dummy_firmware.bin"}
        self.client.firmware_data = b''
        self.client._TBDeviceMqttClient__current_chunk = 0
        self.client._TBDeviceMqttClient__firmware_request_id = 1
        self.client._TBDeviceMqttClient__service_loop = Thread(target=lambda: None)
        self.client._TBDeviceMqttClient__updating_thread = Thread(target=lambda: None)
        self.client._publish_data = MagicMock()
        if not hasattr(self.client, '_client'):
            self.client._client = self.mock_mqtt_client

    def test_connect(self):
        self.client.connect()
        self.mock_mqtt_client.connect.assert_called_with('thingsboard_host', 1883, keepalive=120)
        self.mock_mqtt_client.loop_start.assert_called()

    def test_disconnect(self):
        self.client.disconnect()
        self.mock_mqtt_client.disconnect.assert_called()
        self.mock_mqtt_client.loop_stop.assert_called()

    def test_send_telemetry(self):
        telemetry = {'temp': 22}
        self.client.send_telemetry(telemetry)
        self.client._publish_data.assert_called_with([telemetry], 'v1/devices/me/telemetry', 1, True)

    def test_get_firmware_update(self):
        self.client._client.subscribe = MagicMock()
        self.client.send_telemetry = MagicMock()
        self.client.get_firmware_update()
        self.client._client.subscribe.assert_called_with('v2/fw/response/+')
        self.client.send_telemetry.assert_called()
        self.client._publish_data.assert_called()

class TestProcessFirmwareVerifiedBranch(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient("localhost", 1883, "dummy_token")
        self.client.send_telemetry = MagicMock()
        self.client.current_firmware_info = {
            "current_fw_title": "OldFirmware",
            "current_fw_version": "v0",
            "fw_state": "IDLE"
        }
        self.client.firmware_info = {
            "fw_checksum": "valid_checksum",
            "fw_checksum_algorithm": "SHA256",
            "fw_title": "Firmware Title",
            "fw_version": "v1.0"
        }
        self.client.firmware_data = b'binary data'
        self.client.firmware_received = False

    @patch("tb_device_mqtt.sleep", return_value=None)
    @patch("tb_device_mqtt.verify_checksum", return_value=True)
    def test_process_firmware_verified(self, mock_verify, mock_sleep):
        self.client._TBDeviceMqttClient__process_firmware()
        self.assertEqual(self.client.current_firmware_info["fw_state"], "VERIFIED")
        self.assertGreaterEqual(self.client.send_telemetry.call_count, 2)
        self.assertTrue(self.client.firmware_received)

if __name__ == '__main__':
    unittest.main()
