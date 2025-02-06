# Copyright 2025. ThingsBoard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest.mock import MagicMock, patch
from tb_device_mqtt import TBDeviceMqttClient, TBTimeoutException


class TestTBDeviceMqttClient(unittest.TestCase):

    @patch('tb_device_mqtt.paho.Client')
    def setUp(self, mock_paho_client):
        self.mock_mqtt_client = mock_paho_client.return_value
        self.client = TBDeviceMqttClient(host='host', port=1883, username='token',
                                         password=None)

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
        self.client.send_telemetry({'temp': 22})
        self.client._publish_data.assert_called_with([{'temp': 22}], 'v1/devices/me/telemetry', 1, True)

    def test_get_firmware_update(self):
        self.client._client.subscribe = MagicMock()
        self.client.send_telemetry = MagicMock()
        self.client._publish_data = MagicMock()

        self.client.get_firmware_update()

        self.client._client.subscribe.assert_called_with('v2/fw/response/+')
        self.client.send_telemetry.assert_called()
        self.client._publish_data.assert_called()

    def test_firmware_download_process(self):
        self.client._publish_data = MagicMock()

        self.client.firmware_info = {
            "fw_title": "NewFirmware",
            "fw_version": "2.0",
            "fw_size": 1024,
            "fw_checksum": "abc123",
            "fw_checksum_algorithm": "SHA256"
        }

        self.client._TBDeviceMqttClient__current_chunk = 0
        self.client._TBDeviceMqttClient__firmware_request_id = 1

        self.client._TBDeviceMqttClient__get_firmware()

        self.client._publish_data.assert_called()

    def test_firmware_verification_success(self):
        self.client._publish_data = MagicMock()
        self.client.firmware_data = b'binary data'
        self.client.firmware_info = {
            "fw_title": "NewFirmware",
            "fw_version": "2.0",
            "fw_checksum": "valid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }

        self.client._TBDeviceMqttClient__process_firmware()

        self.client._publish_data.assert_called()

    def test_firmware_verification_failure(self):
        self.client._publish_data = MagicMock()
        self.client.firmware_data = b'corrupt data'
        self.client.firmware_info = {
            "fw_title": "NewFirmware",
            "fw_version": "2.0",
            "fw_checksum": "invalid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }

        self.client._TBDeviceMqttClient__process_firmware()

        self.client._publish_data.assert_called()

    def test_firmware_state_transition(self):
        self.client._publish_data = MagicMock()
        self.client.current_firmware_info = {
            "current_fw_title": "OldFirmware",
            "current_fw_version": "1.0",
            "fw_state": "IDLE"
        }

        self.client.firmware_received = True
        self.client._TBDeviceMqttClient__service_loop()
        self.client._publish_data.assert_called()

    def test_firmware_request_info(self):
        self.client._publish_data = MagicMock()
        self.client._TBDeviceMqttClient__request_firmware_info()

        self.client._publish_data.assert_called()

    def test_firmware_chunk_reception(self):
        self.client._publish_data = MagicMock()
        self.client._TBDeviceMqttClient__get_firmware()

        self.client._publish_data.assert_called()

    def test_timeout_exception(self):
        with self.assertRaises(TBTimeoutException):
            raise TBTimeoutException("Timeout occurred")

if __name__ == '__main__':
    unittest.main()
