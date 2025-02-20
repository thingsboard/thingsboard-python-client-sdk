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
from time import sleep
from unittest.mock import MagicMock, patch
from tb_device_mqtt import TBDeviceMqttClient, RateLimit, TBPublishInfo, TBTimeoutException, TBQoSException
from threading import RLock


@patch('tb_device_mqtt.log')
class TestTBDeviceMqttClientSendRpcReply(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient(host="fake", port=0, username="", password="")

    @patch.object(TBDeviceMqttClient, '_publish_data', autospec=True)
    def test_send_rpc_reply_qos_invalid(self, mock_publish_data, mock_log):
        result = self.client.send_rpc_reply("some_req_id", {"some": "response"}, quality_of_service=2)
        self.assertIsNone(result)
        mock_log.error.assert_called_with("Quality of service (qos) value must be 0 or 1")
        mock_publish_data.assert_not_called()

    @patch.object(TBDeviceMqttClient, '_publish_data', autospec=True)
    def test_send_rpc_reply_qos_ok_no_wait(self, mock_publish_data, mock_log):
        mock_info = MagicMock()
        mock_publish_data.return_value = mock_info

        result = self.client.send_rpc_reply("another_req_id", {"hello": "world"}, quality_of_service=0)
        self.assertEqual(result, mock_info)

        mock_publish_data.assert_called_with(
            self.client,
            {"hello": "world"},
            "v1/devices/me/rpc/response/another_req_id",
            0
        )
        mock_log.error.assert_not_called()
        mock_info.get.assert_not_called()

    @patch.object(TBDeviceMqttClient, '_publish_data', autospec=True)
    def test_send_rpc_reply_qos_ok_wait_publish(self, mock_publish_data, mock_log):
        mock_info = MagicMock()
        mock_publish_data.return_value = mock_info

        result = self.client.send_rpc_reply("req_wait", {"val": 42}, quality_of_service=1, wait_for_publish=True)
        self.assertEqual(result, mock_info)

        mock_publish_data.assert_called_with(
            self.client,
            {"val": 42},
            "v1/devices/me/rpc/response/req_wait",
            1
        )
        mock_info.get.assert_called_once()
        mock_log.error.assert_not_called()
class TestTimeoutCheck(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient('fake_host', username="dummy_token", password="dummy")
        self.client.stopped = False
        self.client._lock = RLock()
        self.client._TBDeviceMqttClient__attrs_request_timeout = {}
        self.client._attr_request_dict = {}

    @patch('tb_device_mqtt.sleep', autospec=True)
    @patch('tb_device_mqtt.monotonic', autospec=True)
    def test_timeout_check_callback(self, mock_monotonic, mock_sleep):
        self.client._TBDeviceMqttClient__attrs_request_timeout = {42: 100}
        mock_callback = MagicMock()
        self.client._attr_request_dict = {42: mock_callback}
        mock_monotonic.return_value = 200
        def sleep_side_effect(duration):
            self.client.stopped = True
            return None
        mock_sleep.side_effect = sleep_side_effect

        self.client._TBDeviceMqttClient__timeout_check()

        mock_callback.assert_called_once()
        args, kwargs = mock_callback.call_args
        self.assertIsNone(args[0])
        self.assertIsInstance(args[1], Exception)
        self.assertIn("Timeout while waiting for a reply", str(args[1]))

        self.assertNotIn(42, self.client._TBDeviceMqttClient__attrs_request_timeout)
