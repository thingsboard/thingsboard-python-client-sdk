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
    RESULT_CODES,
    FW_CHECKSUM_ALG_ATTR,
    FW_CHECKSUM_ATTR,
    FW_VERSION_ATTR,
    FW_TITLE_ATTR,
    FW_STATE_ATTR
)
from paho.mqtt.client import ReasonCodes

class TestFirmwareUpdateBranch(unittest.TestCase):
    @patch('tb_device_mqtt.sleep', return_value=None)
    @patch('tb_device_mqtt.log.debug', autospec=True)
    def test_firmware_update_branch(self, _, mock_sleep):
        c = TBDeviceMqttClient('fake_host', username="dummy_token", password="dummy")
        c._TBDeviceMqttClient__service_loop = lambda: None
        c._TBDeviceMqttClient__timeout_check = lambda: None
        c._messages_rate_limit = MagicMock()
        c.current_firmware_info = {"current_"+FW_VERSION_ATTR: "v0", FW_STATE_ATTR: "IDLE"}
        c.firmware_data = b"old_data"
        c._TBDeviceMqttClient__current_chunk = 2
        c._TBDeviceMqttClient__firmware_request_id = 0
        c._TBDeviceMqttClient__chunk_size = 128
        c._TBDeviceMqttClient__target_firmware_length = 0
        c.send_telemetry = MagicMock()
        c._TBDeviceMqttClient__get_firmware = MagicMock()
        m = MagicMock()
        m.topic = "v1/devices/me/attributes_update"
        p = {"fw_version": "v1","fw_title": "TestFirmware","fw_size": 900}
        m.payload = orjson.dumps(p)
        c._on_decoded_message({}, m)
        c.stopped = True
        c._messages_rate_limit.increase_rate_limit_counter.assert_called_once()
        self.assertEqual(c.firmware_data, b"")
        self.assertEqual(c._TBDeviceMqttClient__current_chunk, 0)
        self.assertEqual(c.current_firmware_info[FW_STATE_ATTR], "DOWNLOADING")
        c.send_telemetry.assert_called_once_with(c.current_firmware_info)
        r = any(a and (a[0] == 1 or a[0] == 1.0) for a, _ in mock_sleep.call_args_list)
        self.assertTrue(r)
        self.assertEqual(c._TBDeviceMqttClient__firmware_request_id, 1)
        self.assertEqual(c._TBDeviceMqttClient__target_firmware_length, 900)
        self.assertEqual(c._TBDeviceMqttClient__chunk_count, ceil(900 / 128))
        c._TBDeviceMqttClient__get_firmware.assert_called_once()

class TestTBDeviceMqttClientOnConnect(unittest.TestCase):
    @patch('tb_device_mqtt.log')
    def test_on_connect_success(self, m):
        c = TBDeviceMqttClient("thingsboard_host", 1883, "token")
        c._subscribe_to_topic = MagicMock()
        c._on_connect(client=None, userdata=None, flags=None, result_code=0)
        self.assertTrue(c._TBDeviceMqttClient__is_connected)
        m.error.assert_not_called()
        e = [
            ('v1/devices/me/attributes', c.quality_of_service),
            ('v1/devices/me/attributes/response/+', c.quality_of_service),
            ('v1/devices/me/rpc/request/+', c.quality_of_service),
            ('v1/devices/me/rpc/response/+', c.quality_of_service),
        ]
        c._subscribe_to_topic.assert_has_calls([call(x, qos=y) for x,y in e], any_order=False)
        self.assertTrue(c._TBDeviceMqttClient__request_service_configuration_required)

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_known_code(self, m):
        c = TBDeviceMqttClient("thingsboard_host", 1883, "token")
        c._on_connect(client=None, userdata=None, flags=None, result_code=1)
        self.assertFalse(c._TBDeviceMqttClient__is_connected)
        m.error.assert_called_once()

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_unknown_code(self, m):
        c = TBDeviceMqttClient("thingsboard_host", 1883, "token")
        c._on_connect(client=None, userdata=None, flags=None, result_code=999)
        self.assertFalse(c._TBDeviceMqttClient__is_connected)
        m.error.assert_called_once()

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_reasoncodes(self, m):
        c = TBDeviceMqttClient("thingsboard_host", 1883, "token")
        r = MagicMock(spec=ReasonCodes)
        r.getName.return_value = "SomeError"
        c._on_connect(client=None, userdata=None, flags=None, result_code=r)
        self.assertFalse(c._TBDeviceMqttClient__is_connected)
        m.error.assert_called_once()

    @patch.object(TBDeviceMqttClient, '_TBDeviceMqttClient__process_firmware', autospec=True)
    @patch.object(TBDeviceMqttClient, '_TBDeviceMqttClient__get_firmware', autospec=True)
    def test_on_message_firmware_update_flow(self, g, p):
        c = TBDeviceMqttClient("fake", 0, "", "")
        c._TBDeviceMqttClient__firmware_request_id = 1
        c.firmware_data = b""
        c._TBDeviceMqttClient__current_chunk = 0
        c._TBDeviceMqttClient__target_firmware_length = 10
        c.firmware_info = {"fw_size": 10}
        c._decode = MagicMock(return_value={"decoded":"x"})
        c._on_decoded_message = MagicMock()
        m = MagicMock()
        m.topic = "v2/fw/response/1/chunk/0"
        m.payload = b"12345"
        c._on_message(None, None, m)
        self.assertEqual(c.firmware_data, b"12345")
        self.assertEqual(c._TBDeviceMqttClient__current_chunk, 1)
        g.assert_called_once()
        p.assert_not_called()
        g.reset_mock()
        m.payload = b"67890"
        c._on_message(None, None, m)
        self.assertEqual(c.firmware_data, b"1234567890")
        self.assertEqual(c._TBDeviceMqttClient__current_chunk, 2)
        p.assert_called_once()
        g.assert_not_called()
        m.topic = "v2/fw/response/999/chunk/0"
        m.payload = b'{"fake": "payload"}'
        c._on_message(None, None, m)
        c._decode.assert_called_once_with(m)
        c._on_decoded_message.assert_called_once_with({"decoded":"x"}, m)

    @patch('tb_device_mqtt.log')
    def test_on_connect_callback_with_tb_client(self, m):
        c = TBDeviceMqttClient("thingsboard_host", 1883, "token")
        def cb(a, b, f, r, *args, tb_client=None):
            self.assertIsNotNone(tb_client)
            self.assertEqual(tb_client, c)
        c._TBDeviceMqttClient__connect_callback = cb
        c._on_connect(client=None, userdata="x", flags="y", result_code=0)
        m.error.assert_not_called()

    @patch('tb_device_mqtt.log')
    def test_on_connect_callback_without_tb_client(self, m):
        c = TBDeviceMqttClient("thingsboard_host", 1883, "token")
        def cb(a, b, f, r, *args):
            pass
        c._TBDeviceMqttClient__connect_callback = cb
        c._on_connect(client=None, userdata="x", flags="y", result_code=0)
        m.error.assert_not_called()

class TestTBDeviceMqttClient(unittest.TestCase):
    @patch('tb_device_mqtt.paho.Client')
    def setUp(self, mp):
        self.m = mp.return_value
        self.c = TBDeviceMqttClient("thingsboard_host", 1883, "token")
        self.c.firmware_info = {FW_TITLE_ATTR: "dummy_firmware.bin"}
        self.c.firmware_data = b''
        self.c._TBDeviceMqttClient__current_chunk = 0
        self.c._TBDeviceMqttClient__firmware_request_id = 1
        self.c._TBDeviceMqttClient__service_loop = Thread(target=lambda: None)
        self.c._TBDeviceMqttClient__updating_thread = Thread(target=lambda: None)
        self.c._publish_data = MagicMock()
        if not hasattr(self.c, '_client'):
            self.c._client = self.m

    def test_connect(self):
        self.c.connect()
        self.m.connect.assert_called_with("thingsboard_host", 1883, keepalive=120)
        self.m.loop_start.assert_called()

    def test_disconnect(self):
        self.c.disconnect()
        self.m.disconnect.assert_called()
        self.m.loop_stop.assert_called()

    def test_send_telemetry(self):
        t = {'temp': 22}
        self.c.send_telemetry(t)
        self.c._publish_data.assert_called_with([t], 'v1/devices/me/telemetry', 1, True)

    def test_get_firmware_update(self):
        self.c._client.subscribe = MagicMock()
        self.c.send_telemetry = MagicMock()
        self.c.get_firmware_update()
        self.c._client.subscribe.assert_called_with('v2/fw/response/+')
        self.c.send_telemetry.assert_called()
        self.c._publish_data.assert_called()

    def test_firmware_download_process(self):
        self.c.firmware_info = {
            FW_TITLE_ATTR: "dummy_firmware.bin",
            FW_VERSION_ATTR: "2.0",
            "fw_size": 1024,
            "fw_checksum": "abc123",
            "fw_checksum_algorithm": "SHA256"
        }
        self.c._TBDeviceMqttClient__current_chunk = 0
        self.c._TBDeviceMqttClient__firmware_request_id = 1
        self.c._TBDeviceMqttClient__get_firmware()
        self.c._publish_data.assert_called()

    def test_firmware_verification_success(self):
        self.c.firmware_data = b'binary data'
        self.c.firmware_info = {
            FW_TITLE_ATTR: "dummy_firmware.bin",
            FW_VERSION_ATTR: "2.0",
            "fw_checksum": "valid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }
        self.c._TBDeviceMqttClient__process_firmware()
        self.c._publish_data.assert_called()

    def test_firmware_verification_failure(self):
        self.c.firmware_data = b'corrupt data'
        self.c.firmware_info = {
            FW_TITLE_ATTR: "dummy_firmware.bin",
            FW_VERSION_ATTR: "2.0",
            "fw_checksum": "invalid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }
        self.c._TBDeviceMqttClient__process_firmware()
        self.c._publish_data.assert_called()

    def test_firmware_state_transition(self):
        self.c._publish_data.reset_mock()
        self.c.current_firmware_info = {
            "current_fw_title": "OldFirmware",
            "current_fw_version": "1.0",
            "fw_state": "IDLE"
        }
        self.c.firmware_received = True
        self.c.firmware_info[FW_TITLE_ATTR] = "dummy_firmware.bin"
        self.c.firmware_info[FW_VERSION_ATTR] = "dummy_version"
        with patch("builtins.open", new_callable=MagicMock) as o:
            if hasattr(self.c, '_TBDeviceMqttClient__on_firmware_received'):
                self.c._TBDeviceMqttClient__on_firmware_received("dummy_version")
                o.assert_called_with("dummy_firmware.bin", "wb")

    def test_firmware_request_info(self):
        self.c._publish_data.reset_mock()
        self.c._TBDeviceMqttClient__request_firmware_info()
        self.c._publish_data.assert_called()

    def test_firmware_chunk_reception(self):
        self.c._publish_data.reset_mock()
        self.c._TBDeviceMqttClient__get_firmware()
        self.c._publish_data.assert_called()

    def test_timeout_exception(self):
        with self.assertRaises(TBTimeoutException):
            raise TBTimeoutException("Timeout occurred")

class TestProcessFirmwareVerifiedBranch(unittest.TestCase):
    def setUp(self):
        self.c = TBDeviceMqttClient("localhost", 1883, "dummy_token")
        self.c.send_telemetry = MagicMock()
        self.c.current_firmware_info = {
            "current_"+FW_VERSION_ATTR: "InitialVersion",
            FW_STATE_ATTR: "IDLE"
        }
        self.c.firmware_info = {
            FW_CHECKSUM_ATTR: "dummy_checksum",
            FW_CHECKSUM_ALG_ATTR: "dummy_alg",
            FW_TITLE_ATTR: "Firmware Title",
            FW_VERSION_ATTR: "v1.0",
        }
        self.c.firmware_data = b"dummy firmware data"
        self.c.firmware_received = False

    @patch("tb_device_mqtt.sleep", return_value=None)
    @patch("tb_device_mqtt.verify_checksum", return_value=True)
    def test_process_firmware_verified(self, _, __):
        self.c._TBDeviceMqttClient__process_firmware()
        self.assertEqual(self.c.current_firmware_info[FW_STATE_ATTR], "VERIFIED")
        self.assertGreaterEqual(self.c.send_telemetry.call_count, 2)
        self.assertTrue(self.c.firmware_received)

if __name__ == '__main__':
    unittest.main()
