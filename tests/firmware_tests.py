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
from math import ceil
import orjson
from threading import Thread
from tb_device_mqtt import (
    TBDeviceMqttClient,
    TBTimeoutException,
    FW_VERSION_ATTR, FW_TITLE_ATTR, FW_STATE_ATTR
)
from paho.mqtt.client import ReasonCodes


REQUIRED_SHARED_KEYS = "dummy_shared_keys"


class TestFirmwareUpdateBranch(unittest.TestCase):
    @patch('tb_device_mqtt.sleep', return_value=None, autospec=True)
    @patch('tb_device_mqtt.log.debug', autospec=True)
    def test_firmware_update_branch(self, _, mock_sleep):
        client = TBDeviceMqttClient('fake_host', username="dummy_token", password="dummy")

        client._messages_rate_limit = MagicMock()

        client.current_firmware_info = {
            "current_" + FW_VERSION_ATTR: "v0",
            FW_STATE_ATTR: "IDLE"
        }
        client.firmware_data = b"old_data"
        client._TBDeviceMqttClient__current_chunk = 2
        client._TBDeviceMqttClient__firmware_request_id = 0
        client._TBDeviceMqttClient__chunk_size = 128
        client._TBDeviceMqttClient__target_firmware_length = 0

        client.send_telemetry = MagicMock()
        client._TBDeviceMqttClient__get_firmware = MagicMock()

        message_mock = MagicMock()
        message_mock.topic = "v1/devices/me/attributes_update"
        payload_dict = {
            "fw_version": "v1",
            "fw_title": "TestFirmware",
            "fw_size": 900
        }
        message_mock.payload = orjson.dumps(payload_dict)

        client._on_decoded_message({}, message_mock)
        client.stopped = True

        client._messages_rate_limit.increase_rate_limit_counter.assert_called_once()

        self.assertEqual(client.firmware_data, b"")
        self.assertEqual(client._TBDeviceMqttClient__current_chunk, 0)
        self.assertEqual(client.current_firmware_info[FW_STATE_ATTR], "DOWNLOADING")

        client.send_telemetry.assert_called_once_with(client.current_firmware_info)

        client._TBDeviceMqttClient__get_firmware.assert_called_once()

        self.assertEqual(client._TBDeviceMqttClient__firmware_request_id, 1)
        self.assertEqual(client._TBDeviceMqttClient__target_firmware_length, 900)
        self.assertEqual(client._TBDeviceMqttClient__chunk_count, ceil(900 / 128))
        client._TBDeviceMqttClient__get_firmware.assert_called_once()


class TestTBDeviceMqttClient(unittest.TestCase):
    @patch('tb_device_mqtt.paho.Client')
    def setUp(self, mock_paho_client):
        self.mock_mqtt_client = mock_paho_client.return_value
        self.client = TBDeviceMqttClient(
            host='your_host',
            port=1883,
            username='your_token',
            password=None
        )
        self.client.firmware_info = {FW_TITLE_ATTR: "dummy_firmware.bin"}
        self.client.firmware_data = b''
        self.client._TBDeviceMqttClient__current_chunk = 0
        self.client._TBDeviceMqttClient__firmware_request_id = 1
        self.client._TBDeviceMqttClient__updating_thread = Thread(target=lambda: None)
        self.client._publish_data = MagicMock()

        if not hasattr(self.client, '_client'):
            self.client._client = self.mock_mqtt_client

    def test_get_firmware_update(self):
        self.client._client.subscribe = MagicMock()
        self.client.send_telemetry = MagicMock()
        self.client.get_firmware_update()
        self.client._client.subscribe.assert_called_with('v2/fw/response/+')
        self.client.send_telemetry.assert_called()
        self.client._publish_data.assert_called()

    def test_firmware_download_process(self):
        self.client.firmware_info = {
            FW_TITLE_ATTR: "dummy_firmware.bin",
            FW_VERSION_ATTR: "2.0",
            "fw_size": 1024,
            "fw_checksum": "abc123",
            "fw_checksum_algorithm": "SHA256"
        }
        self.client._TBDeviceMqttClient__current_chunk = 0
        self.client._TBDeviceMqttClient__firmware_request_id = 1
        self.client._TBDeviceMqttClient__get_firmware()
        self.client._publish_data.assert_called()

    def test_firmware_verification_success(self):
        self.client.firmware_data = b'binary data'
        self.client.firmware_info = {
            FW_TITLE_ATTR: "dummy_firmware.bin",
            FW_VERSION_ATTR: "2.0",
            "fw_checksum": "valid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }
        self.client._TBDeviceMqttClient__process_firmware()
        self.client._publish_data.assert_called()

    def test_firmware_verification_failure(self):
        self.client.firmware_data = b'corrupt data'
        self.client.firmware_info = {
            FW_TITLE_ATTR: "dummy_firmware.bin",
            FW_VERSION_ATTR: "2.0",
            "fw_checksum": "invalid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }
        self.client._TBDeviceMqttClient__process_firmware()
        self.client._publish_data.assert_called()

    def test_firmware_state_transition(self):
        self.client._publish_data.reset_mock()
        self.client.current_firmware_info = {
            "current_fw_title": "OldFirmware",
            "current_fw_version": "1.0",
            "fw_state": "IDLE"
        }
        self.client.firmware_received = True
        self.client.firmware_info[FW_TITLE_ATTR] = "dummy_firmware.bin"
        self.client.firmware_info[FW_VERSION_ATTR] = "dummy_version"

    def test_firmware_request_info(self):
        self.client._publish_data.reset_mock()
        self.client._TBDeviceMqttClient__request_firmware_info()
        self.client._publish_data.assert_called()

    def test_firmware_chunk_reception_detailed(self):
        self.client._publish_data.reset_mock()
        self.client._TBDeviceMqttClient__get_firmware()
        self.client._publish_data.assert_called()

    @patch.object(TBDeviceMqttClient, 'send_telemetry')
    def test_process_firmware_telemetry_calls(self, mock_send_telemetry):
        self.client.firmware_data = b"some_firmware_data"
        self.client.firmware_info = {
            FW_TITLE_ATTR: "dummy_firmware.bin",
            FW_VERSION_ATTR: "2.0",
            "fw_checksum": "valid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }

        self.client._TBDeviceMqttClient__process_firmware()

        self.assertEqual(
            mock_send_telemetry.call_count,
            2,
            "Two calls to send_telemetry are expected in the current firmware implementation"
        )

        expected_calls = [
            call({"current_fw_title": "Initial", "current_fw_version": "v0", "fw_state": "FAILED"}),
            call({"current_fw_title": "Initial", "current_fw_version": "v0", "fw_state": "FAILED"})
        ]
        mock_send_telemetry.assert_has_calls(expected_calls, any_order=False)


class TestFirmwareChunkReception(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient(host="localhost", port=1883)
        self.client._TBDeviceMqttClient__firmware_request_id = 1
        self.client._TBDeviceMqttClient__current_chunk = 0

    @patch.object(TBDeviceMqttClient, '_publish_data')
    def test_firmware_chunk_reception(self, mock_publish_data):
        self.client._TBDeviceMqttClient__chunk_size = 128
        self.client.firmware_info = {
            "fw_size": 300,
            "fw_title": "SomeFirmware",
            "fw_checksum": "12345",
            "fw_checksum_algorithm": "SHA256"
        }
        self.client._TBDeviceMqttClient__get_firmware()
        expected_calls = [
            call(b'128', 'v2/fw/request/1/chunk/0', 1)
        ]
        self.assertEqual(mock_publish_data.call_count, 1, "Only one chunk request is expected")
        mock_publish_data.assert_has_calls(expected_calls, any_order=False)

        self.assertEqual(self.client._TBDeviceMqttClient__current_chunk, 0,
                         "The current_chunk should not change if the method only requests chunks.")


class TestFirmwareUpdate(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient(host="localhost", port=1883)
        self.client._TBDeviceMqttClient__process_firmware = MagicMock()
        self.client._TBDeviceMqttClient__get_firmware = MagicMock()

        self.client._TBDeviceMqttClient__firmware_request_id = 1
        self.client._TBDeviceMqttClient__current_chunk = 0
        self.client._TBDeviceMqttClient__target_firmware_length = 10

        self.client.firmware_data = b''

    def test_incomplete_firmware_chunk(self):
        chunk_data = b'abcde'
        message = MagicMock()
        message.topic = "v2/fw/response/1/chunk/0"
        message.payload = chunk_data

        self.client._on_message(None, None, message)
        self.assertEqual(self.client.firmware_data, b'abcde')
        self.assertEqual(self.client._TBDeviceMqttClient__current_chunk, 1)
        self.client._TBDeviceMqttClient__process_firmware.assert_not_called()
        self.client._TBDeviceMqttClient__get_firmware.assert_called_once()

    def test_complete_firmware_chunk(self):
        self.client.firmware_data = b'abcde'
        self.client._TBDeviceMqttClient__current_chunk = 1

        chunk_data = b'12345'
        message = MagicMock()
        message.topic = "v2/fw/response/1/chunk/1"
        message.payload = chunk_data

        self.client._on_message(None, None, message)

        self.assertEqual(self.client.firmware_data, b'abcde12345')
        self.assertEqual(self.client._TBDeviceMqttClient__current_chunk, 2)

        self.client._TBDeviceMqttClient__process_firmware.assert_called_once()
        self.client._TBDeviceMqttClient__get_firmware.assert_not_called()


if __name__ == '__main__':
    unittest.main()
