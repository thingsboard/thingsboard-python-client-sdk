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
from unittest.mock import patch, MagicMock

from paho.mqtt.client import MQTT_ERR_QUEUE_SIZE
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo, RateLimit
from tb_gateway_mqtt import TBGatewayMqttClient


class TestSendSplitMessageRetry(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient('fake_host', username="dummy_token", password="dummy")
        self.fake_publish_ok = MagicMock()
        self.fake_publish_ok.rc = 0
        self.fake_publish_queue = MagicMock()
        self.fake_publish_queue.rc = MQTT_ERR_QUEUE_SIZE
        self.client._client.publish = MagicMock()
        self.client.stopped = False
        self.client._wait_until_current_queued_messages_processed = MagicMock()
        self.client._wait_for_rate_limit_released = MagicMock(return_value=False)
        self.client._TBDeviceMqttClient__error_logged = 0
        self.dp_rate_limit = RateLimit("10:1")
        self.msg_rate_limit = RateLimit("10:1")

        self.client.max_payload_size = 999999

    @patch.object(TBDeviceMqttClient, '_split_message', autospec=True)
    @patch.object(TBDeviceMqttClient, '_TBDeviceMqttClient__send_split_message', autospec=True)
    def test_send_publish_device_block_no_attributes(self, mock_send_split, mock_split_message):
        data = {
            "MyDevice": {
                "temp": 22,
                "humidity": 55
            }
        }
        kwargs = {
            "payload": data,
            "topic": "v1/devices/me/telemetry"
        }
        timeout = 10
        device = "MyDevice"

        mock_split_message.return_value = [
            {"data": [{"temp": 22}], "datapoints": 1},
            {"data": [{"humidity": 55}], "datapoints": 1}
        ]

        result = self.client._TBDeviceMqttClient__send_publish_with_limitations(
            kwargs=kwargs,
            timeout=timeout,
            device=device,
            msg_rate_limit=self.msg_rate_limit,
            dp_rate_limit=self.dp_rate_limit
        )

        mock_split_message.assert_called_once_with(
            data["MyDevice"],
            self.dp_rate_limit.get_minimal_limit(),
            self.client.max_payload_size
        )

        calls = mock_send_split.call_args_list
        self.assertEqual(len(calls), 2, "Expect 2 calls to __send_split_message, because split_message returned 2 parts")

        first_call_args, _ = calls[0]
        part_1 = first_call_args[2]
        self.assertIn("message", part_1)
        self.assertEqual(part_1["datapoints"], 1)
        self.assertIn("MyDevice", part_1["message"])
        self.assertEqual(part_1["message"]["MyDevice"], [{"temp": 22}])

        second_call_args, _ = calls[1]
        part_2 = second_call_args[2]
        self.assertEqual(part_2["datapoints"], 1)
        self.assertIn("MyDevice", part_2["message"])
        self.assertEqual(part_2["message"]["MyDevice"], [{"humidity": 55}])

        self.assertIsInstance(result, TBPublishInfo)

    @patch('tb_device_mqtt.sleep', autospec=True)
    @patch('tb_device_mqtt.log.warning', autospec=True)
    def test_send_split_message_queue_size_retry(self, mock_log_warning, mock_sleep):
        part = {'datapoints': 3, 'message': {"foo": "bar"}}
        kwargs = {}
        timeout = 10
        device = "device2"
        topic = "test/topic2"
        msg_rate_limit = MagicMock()
        dp_rate_limit = MagicMock()
        msg_rate_limit.has_limit.return_value = True
        dp_rate_limit.has_limit.return_value = True
        self.client._wait_for_rate_limit_released = MagicMock(return_value=False)
        self.client._client.publish.side_effect = [
            self.fake_publish_queue, self.fake_publish_queue, self.fake_publish_ok
        ]
        self.client._TBDeviceMqttClient__error_logged = 0
        with patch('tb_device_mqtt.monotonic', side_effect=[0, 12, 12, 12]):
            results = []
            ret = self.client._TBDeviceMqttClient__send_split_message(
                results, part, kwargs, timeout, device, msg_rate_limit, dp_rate_limit, topic
            )
        self.assertEqual(self.client._client.publish.call_count, 3)
        mock_log_warning.assert_called()
        self.assertIsNone(ret)
        self.assertIn(self.fake_publish_ok, results)


class TestWaitUntilQueuedMessagesProcessed(unittest.TestCase):
    @patch('tb_device_mqtt.sleep', autospec=True)
    @patch('tb_device_mqtt.logging.getLogger', autospec=True)
    @patch('tb_device_mqtt.monotonic')
    def test_wait_until_current_queued_messages_processed_logging(self, mock_monotonic, mock_getLogger, mock_sleep):
        client = TBDeviceMqttClient('fake_host', username="dummy_token", password="dummy")
        fake_client = MagicMock()
        fake_client._out_messages = [1, 2, 3, 4, 5, 6]
        fake_client._max_inflight_messages = 5
        client._client = fake_client
        client.stopped = False
        client.is_connected = MagicMock(return_value=True)
        mock_monotonic.side_effect = [0, 6, 6, 1000]
        fake_logger = MagicMock()
        mock_getLogger.return_value = fake_logger

        client._wait_until_current_queued_messages_processed()

        fake_logger.debug.assert_called()

        mock_sleep.assert_called_with(0.001)

    def test_single_value_case(self):
        message_pack = {
            "ts": 123456789,
            "values": {
                "temp": 42
            }
        }

        result = TBDeviceMqttClient._split_message(message_pack, 10, 999999)

        self.assertEqual(len(result), 1)
        chunk = result[0]
        self.assertIn("data", chunk)
        self.assertIn("datapoints", chunk)
        self.assertEqual(chunk["datapoints"], 1)
        self.assertEqual(len(chunk["data"]), 1)
        record = chunk["data"][0]
        self.assertEqual(record.get("ts"), 123456789)
        self.assertEqual(record.get("values"), {"temp": 42})

    def test_ts_changed_with_metadata(self):
        message_pack = [
            {
                "ts": 1000,
                "values": {"temp": 10},
                "metadata": {"info": "first"}
            },
            {
                "ts": 2000,
                "values": {"temp": 20},
                "metadata": {"info": "second"}
            }
        ]
        result = TBDeviceMqttClient._split_message(message_pack, 10, 999999)
        self.assertGreaterEqual(len(result), 2)

        chunk0 = result[0]
        data0 = chunk0["data"][0]
        self.assertEqual(data0["ts"], 1000)
        self.assertIn("values", data0)
        self.assertEqual(data0["values"], {"temp": 10})

        chunk1 = result[1]
        data1 = chunk1["data"][0]
        self.assertEqual(data1["ts"], 2000)
        self.assertIn("values", data1)
        self.assertEqual(data1["values"], {"temp": 20})

    def test_message_item_values_added(self):
        message_pack = {
            "ts": 111,
            "values": {
                "temp": 30,
                "humidity": 40
            },
            "metadata": {"info": "some_meta"}
        }
        result = TBDeviceMqttClient._split_message(message_pack, 100, 999999)
        self.assertEqual(len(result), 1)
        chunk = result[0]
        self.assertEqual(chunk["datapoints"], 2)
        data_list = chunk["data"]
        self.assertEqual(len(data_list), 1)
        record = data_list[0]
        self.assertEqual(record["ts"], 111)
        self.assertEqual(record["values"], {"temp": 30, "humidity": 40})

    def test_last_block_leftover_with_metadata(self):
        message_pack = [
            {
                "ts": 111,
                "values": {"temp": 1},
                "metadata": {"info": "testmeta1"}
            },
            {
                "ts": 111,
                "values": {"pressure": 101},
                "metadata": {"info": "testmeta2"}
            }
        ]
        result = TBDeviceMqttClient._split_message(message_pack, 100, 999999)

        self.assertGreaterEqual(len(result), 1)
        last_chunk = result[-1]
        self.assertIn("data", last_chunk)
        self.assertIn("datapoints", last_chunk)
        data_list = last_chunk["data"]
        self.assertTrue(len(data_list) >= 1)
        found_pressure = any("values" in rec and rec["values"].get("pressure") == 101 for rec in data_list)
        self.assertTrue(found_pressure, "Should see ‘pressure’:101 in leftover")

    def test_ts_to_write_branch(self):
        message1 = {
            "ts": 1000,
            "values": {"a": "A", "b": "B"}
        }
        message2 = {
            "ts": 2000,
            "values": {"c": "C", "d": "D"},
            "metadata": "meta2"
        }
        message_pack = [message1, message2]
        datapoints_max_count = 10
        max_payload_size = 50

        with patch("tb_device_mqtt.TBDeviceMqttClient._datapoints_limit_reached", return_value=True), \
                patch("tb_device_mqtt.TBDeviceMqttClient._payload_size_limit_reached", return_value=False):
            result = TBDeviceMqttClient._split_message(message_pack, datapoints_max_count, max_payload_size)

        found = False
        for split in result:
            data_list = split.get("data", [])
            for chunk in data_list:
                if chunk.get("metadata") == "meta2" and chunk.get("ts") == 1000:
                    found = True
        self.assertTrue(found, "A fragment with ts equal to 1000 and metadata “meta2” was not found")

if __name__ == "__main__":
    unittest.main()
