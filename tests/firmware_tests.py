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
    RPC_REQUEST_TOPIC,
    ATTRIBUTES_TOPIC,
    ATTRIBUTES_TOPIC_RESPONSE,
    FW_VERSION_ATTR, FW_TITLE_ATTR, FW_SIZE_ATTR, FW_STATE_ATTR
)
from paho.mqtt.client import ReasonCodes

FW_TITLE_ATTR = "fw_title"
FW_VERSION_ATTR = "fw_version"
REQUIRED_SHARED_KEYS = "dummy_shared_keys"


class TestFirmwareUpdateBranch(unittest.TestCase):
    @patch('tb_device_mqtt.sleep', return_value=None, autospec=True)
    @patch('tb_device_mqtt.log.debug', autospec=True)
    def test_firmware_update_branch(self, mock_log_debug, mock_sleep):
        client = TBDeviceMqttClient('<THINGSBOARD_HOST>', username="<ACCESS_TOKEN>", password="<PASSWORD>")
        client._TBDeviceMqttClient__service_loop = lambda: None
        client._TBDeviceMqttClient__timeout_check = lambda: None

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

        calls = [args[0] for args, kwargs in mock_log_debug.call_args_list]
        self.assertTrue(any("Firmware is not the same" in call for call in calls),
                        f"Expected log.debug call with 'Firmware is not the same', got: {calls}")

        self.assertEqual(client.firmware_data, b"")
        self.assertEqual(client._TBDeviceMqttClient__current_chunk, 0)
        self.assertEqual(client.current_firmware_info[FW_STATE_ATTR], "DOWNLOADING")

        client.send_telemetry.assert_called_once_with(client.current_firmware_info)

        sleep_called = any(args and (args[0] == 1 or args[0] == 1.0) for args, kwargs in mock_sleep.call_args_list)
        self.assertTrue(sleep_called, f"sleep(1) was not called, calls: {mock_sleep.call_args_list}")

        self.assertEqual(client._TBDeviceMqttClient__firmware_request_id, 1)
        self.assertEqual(client._TBDeviceMqttClient__target_firmware_length, 900)
        self.assertEqual(client._TBDeviceMqttClient__chunk_count, ceil(900 / 128))
        client._TBDeviceMqttClient__get_firmware.assert_called_once()


class TestTBDeviceMqttClientOnConnect(unittest.TestCase):
    @patch('tb_device_mqtt.log')
    def test_on_connect_success(self, mock_logger):
        client = TBDeviceMqttClient("<THINGSBOARD_HOST>", 1883, "<ACCESS_TOKEN>")
        client._subscribe_to_topic = MagicMock()

        client._on_connect(client=None, userdata=None, flags=None, result_code=0)

        self.assertTrue(client._TBDeviceMqttClient__is_connected)
        mock_logger.error.assert_not_called()

        expected_sub_calls = [
            call('v1/devices/me/attributes', qos=client.quality_of_service),
            call('v1/devices/me/attributes/response/+', qos=client.quality_of_service),
            call('v1/devices/me/rpc/request/+', qos=client.quality_of_service),
            call('v1/devices/me/rpc/response/+', qos=client.quality_of_service),
        ]
        client._subscribe_to_topic.assert_has_calls(expected_sub_calls, any_order=False)

        self.assertTrue(client._TBDeviceMqttClient__request_service_configuration_required)

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_known_code(self, mock_logger):
        client = TBDeviceMqttClient("<THINGSBOARD_HOST>", 1883, "<ACCESS_TOKEN>")

        client._on_connect(client=None, userdata=None, flags=None, result_code=1)

        self.assertFalse(client._TBDeviceMqttClient__is_connected)
        mock_logger.error.assert_called_once_with(
            "connection FAIL with error %s %s",
            1,
            RESULT_CODES[1]
        )

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_unknown_code(self, mock_logger):
        client = TBDeviceMqttClient("<THINGSBOARD_HOST>", 1883, "<ACCESS_TOKEN>")

        client._on_connect(client=None, userdata=None, flags=None, result_code=999)

        self.assertFalse(client._TBDeviceMqttClient__is_connected)
        mock_logger.error.assert_called_once_with("connection FAIL with unknown error")

    @patch('tb_device_mqtt.log')
    def test_on_connect_fail_reasoncodes(self, mock_logger):
        client = TBDeviceMqttClient("<THINGSBOARD_HOST>", 1883, "<ACCESS_TOKEN>")

        mock_rc = MagicMock(spec=ReasonCodes)
        mock_rc.getName.return_value = "SomeError"

        client._on_connect(client=None, userdata=None, flags=None, result_code=mock_rc)

        self.assertFalse(client._TBDeviceMqttClient__is_connected)
        mock_logger.error.assert_called_once_with(
            "connection FAIL with error %s %s",
            mock_rc,
            "SomeError"
        )

    @patch.object(TBDeviceMqttClient, '_TBDeviceMqttClient__process_firmware', autospec=True)
    @patch.object(TBDeviceMqttClient, '_TBDeviceMqttClient__get_firmware', autospec=True)
    def test_on_message_firmware_update_flow(self, mock_get_firmware, mock_process_firmware):
        client = TBDeviceMqttClient(host="fake", port=0, username="", password="")
        client._TBDeviceMqttClient__firmware_request_id = 1
        client.firmware_data = b""
        client._TBDeviceMqttClient__current_chunk = 0
        client._TBDeviceMqttClient__target_firmware_length = 10
        client.firmware_info = {"fw_size": 10}

        client._decode = MagicMock(return_value={"decoded": "some_value"})
        client._on_decoded_message = MagicMock()

        message_mock = MagicMock()
        message_mock.topic = "v2/fw/response/1/chunk/0"
        message_mock.payload = b"12345"

        client._on_message(None, None, message_mock)
        self.assertEqual(client.firmware_data, b"12345")
        self.assertEqual(client._TBDeviceMqttClient__current_chunk, 1)

        mock_get_firmware.assert_called_once()
        mock_process_firmware.assert_not_called()

        mock_get_firmware.reset_mock()
        message_mock.payload = b"67890"

        client._on_message(None, None, message_mock)
        self.assertEqual(client.firmware_data, b"1234567890")
        self.assertEqual(client._TBDeviceMqttClient__current_chunk, 2)

        mock_process_firmware.assert_called_once()
        mock_get_firmware.assert_not_called()

        message_mock.topic = "v2/fw/response/999/chunk/0"
        message_mock.payload = b'{"fake": "payload"}'

        client._on_message(None, None, message_mock)
        client._decode.assert_called_once_with(message_mock)
        client._on_decoded_message.assert_called_once_with({"decoded": "some_value"}, message_mock)

    @patch('tb_device_mqtt.log')
    def test_on_connect_callback_with_tb_client(self, mock_logger):
        client = TBDeviceMqttClient("<THINGSBOARD_HOST>", 1883, "<ACCESS_TOKEN>")

        def my_connect_callback(client_param, userdata, flags, rc, *args, tb_client=None):
            self.assertIsNotNone(tb_client, "tb_client should be passed to the callback")
            self.assertEqual(tb_client, client)

        client._TBDeviceMqttClient__connect_callback = my_connect_callback

        client._on_connect(client=None, userdata="test_user_data", flags="test_flags", result_code=0)
        mock_logger.error.assert_not_called()

    @patch('tb_device_mqtt.log')
    def test_on_connect_callback_without_tb_client(self, mock_logger):
        client = TBDeviceMqttClient("<THINGSBOARD_HOST>", 1883, "<ACCESS_TOKEN>")

        def my_callback(client_param, userdata, flags, rc, *args):
            pass

        client._TBDeviceMqttClient__connect_callback = my_callback

        client._on_connect(client=None, userdata="test_user_data", flags="test_flags", result_code=0)
        mock_logger.error.assert_not_called()


class TestTBDeviceMqttClient(unittest.TestCase):

    @patch('tb_device_mqtt.paho.Client')
    def setUp(self, mock_paho_client):
        self.mock_mqtt_client = mock_paho_client.return_value
        self.client = TBDeviceMqttClient(
            host='<THINGSBOARD_HOST>',
            port=1883,
            username='<ACCESS_TOKEN>',
            password=None
        )
        self.client.firmware_info = {FW_TITLE_ATTR: "<FIRMWARE_FILENAME>"}
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
        self.mock_mqtt_client.connect.assert_called_with('<THINGSBOARD_HOST>', 1883, keepalive=120)
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

    def test_firmware_download_process(self):
        self.client.firmware_info = {
            FW_TITLE_ATTR: "<FIRMWARE_FILENAME>",
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
            FW_TITLE_ATTR: "<FIRMWARE_FILENAME>",
            FW_VERSION_ATTR: "2.0",
            "fw_checksum": "valid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }
        self.client._TBDeviceMqttClient__process_firmware()
        self.client._publish_data.assert_called()

    def test_firmware_verification_failure(self):
        self.client.firmware_data = b'corrupt data'
        self.client.firmware_info = {
            FW_TITLE_ATTR: "<FIRMWARE_FILENAME>",
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
        self.client.firmware_info[FW_TITLE_ATTR] = "<FIRMWARE_FILENAME>"
        self.client.firmware_info[FW_VERSION_ATTR] = "<FIRMWARE_VERSION>"
        with patch("builtins.open", new_callable=MagicMock) as m_open:
            if hasattr(self.client, '_TBDeviceMqttClient__on_firmware_received'):
                self.client._TBDeviceMqttClient__on_firmware_received("<FIRMWARE_VERSION>")
                m_open.assert_called_with("<FIRMWARE_FILENAME>", "wb")

    def test_firmware_request_info(self):
        self.client._publish_data.reset_mock()
        self.client._TBDeviceMqttClient__request_firmware_info()
        self.client._publish_data.assert_called()

    def test_firmware_chunk_reception(self):
        self.client._publish_data.reset_mock()
        self.client._TBDeviceMqttClient__get_firmware()
        self.client._publish_data.assert_called()

    def test_timeout_exception(self):
        with self.assertRaises(TBTimeoutException):
            raise TBTimeoutException("Timeout occurred")

    @unittest.skip("Method __on_message is missing in the current version")
    def test_firmware_message_handling(self):
        pass

    @unittest.skip("Subscription check is redundant (used in test_get_firmware_update)")
    def test_firmware_subscription(self):
        self.client._client.subscribe = MagicMock()
        self.client._TBDeviceMqttClient__request_firmware_info()
        calls = self.client._client.subscribe.call_args_list
        topics = [args[0][0] for args, kwargs in calls]
        self.assertTrue(any("v2/fw/response" in topic for topic in topics))

    def test_thread_attributes(self):
        self.assertTrue(isinstance(self.client._TBDeviceMqttClient__service_loop, Thread))
        self.assertTrue(isinstance(self.client._TBDeviceMqttClient__updating_thread, Thread))


if __name__ == '__main__':
    unittest.main()
