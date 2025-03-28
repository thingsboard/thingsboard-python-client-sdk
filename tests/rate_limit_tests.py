# Copyright 2025. ThingsBoard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
from unittest.mock import MagicMock
from time import sleep
from tb_device_mqtt import RateLimit, TBDeviceMqttClient, TELEMETRY_TOPIC


class TestRateLimit(unittest.TestCase):
    def setUp(self):
        self.rate_limit = RateLimit("10:1,60:10", "test_limit")
        self.client = TBDeviceMqttClient("localhost")

        print("Default messages rate limit:", self.client._messages_rate_limit._rate_limit_dict)
        print("Default telemetry rate limit:", self.client._telemetry_rate_limit._rate_limit_dict)
        print("Default telemetry DP rate limit:", self.client._telemetry_dp_rate_limit._rate_limit_dict)

        self.client._messages_rate_limit.set_limit("10:1,60:10")
        self.client._telemetry_rate_limit.set_limit("10:1,60:10")
        self.client._telemetry_dp_rate_limit.set_limit("10:1,60:10")

    def test_initialization(self):
        self.assertEqual(self.rate_limit.name, "test_limit")
        self.assertEqual(self.rate_limit.percentage, 80)
        self.assertFalse(self.rate_limit._no_limit)

    def test_check_limit_not_reached(self):
        self.assertFalse(self.rate_limit.check_limit_reached())

    def test_increase_counter(self):
        self.rate_limit.increase_rate_limit_counter()
        self.assertEqual(self.rate_limit._rate_limit_dict[1]['counter'], 1)

    def test_limit_reached(self):
        for _ in range(10):
            self.rate_limit.increase_rate_limit_counter()
        self.assertEqual(self.rate_limit.check_limit_reached(), 1)

    def test_limit_reset_after_time(self):
        self.rate_limit.increase_rate_limit_counter(10)
        self.assertEqual(self.rate_limit.check_limit_reached(), 1)
        sleep(1.1)
        self.assertFalse(self.rate_limit.check_limit_reached())

    def test_get_minimal_timeout(self):
        self.assertEqual(self.rate_limit.get_minimal_timeout(), 2)

    def test_set_limit(self):
        self.rate_limit.set_limit("5:1,30:5")
        print("Updated _rate_limit_dict:", self.rate_limit._rate_limit_dict)
        self.assertIn(5, self.rate_limit._rate_limit_dict)

    def test_no_limit(self):
        unlimited = RateLimit("0:0")
        self.assertTrue(unlimited._no_limit)
        self.assertFalse(unlimited.check_limit_reached())

    def test_messages_rate_limit(self):
        self.assertIsInstance(self.client._messages_rate_limit, RateLimit)

    def test_telemetry_limiter(self):
        self.assertIsInstance(self.client._telemetry_rate_limit, RateLimit)

    def test_telemetry_dp_rate_limit(self):
        self.assertIsInstance(self.client._telemetry_dp_rate_limit, RateLimit)

    def test_messages_rate_limit_behavior(self):
        for _ in range(50):
            self.client._messages_rate_limit.increase_rate_limit_counter()
        print("Messages rate limit dict:", self.client._messages_rate_limit._rate_limit_dict)
        self.assertTrue(self.client._messages_rate_limit.check_limit_reached())

    def test_telemetry_rate_limit_behavior(self):
        for _ in range(50):
            self.client._telemetry_rate_limit.increase_rate_limit_counter()
        print("Telemetry rate limit dict:", self.client._telemetry_rate_limit._rate_limit_dict)
        self.assertTrue(self.client._telemetry_rate_limit.check_limit_reached())

    def test_telemetry_dp_rate_limit_behavior(self):
        for _ in range(50):
            self.client._telemetry_dp_rate_limit.increase_rate_limit_counter()
        print("Telemetry DP rate limit dict:", self.client._telemetry_dp_rate_limit._rate_limit_dict)
        self.assertTrue(self.client._telemetry_dp_rate_limit.check_limit_reached())

    def test_rate_limit_90_percent(self):
        rate_limit_90 = RateLimit("10:1,60:10", percentage=90)
        self.assertEqual(rate_limit_90.percentage, 90)

    def test_rate_limit_50_percent(self):
        rate_limit_50 = RateLimit("10:1,60:10", percentage=50)
        self.assertEqual(rate_limit_50.percentage, 50)

    def test_rate_limit_100_percent(self):
        rate_limit_100 = RateLimit("10:1,60:10", percentage=100)
        self.assertEqual(rate_limit_100.percentage, 100)

    def test_mock_rate_limit_methods(self):
        mock_limit = MagicMock(spec=RateLimit)
        mock_limit.check_limit_reached.return_value = False
        self.assertFalse(mock_limit.check_limit_reached())
        mock_limit.increase_rate_limit_counter()
        mock_limit.increase_rate_limit_counter.assert_called()

    def test_counter_increments_correctly(self):
        self.rate_limit.increase_rate_limit_counter()
        self.assertEqual(self.rate_limit._rate_limit_dict[1]['counter'], 1)
        self.rate_limit.increase_rate_limit_counter(5)
        self.assertEqual(self.rate_limit._rate_limit_dict[1]['counter'], 6)

    def test_percentage_affects_limits(self):
        rate_limit_50 = RateLimit("10:1,60:10", percentage=50)
        print("Rate limit dict:", rate_limit_50._rate_limit_dict)

        actual_limits = {k: v['limit'] for k, v in rate_limit_50._rate_limit_dict.items()}
        expected_limits = {
            1: 5,
            10: 30
        }
        self.assertEqual(actual_limits, expected_limits)

    def test_no_limit_behavior(self):
        unlimited = RateLimit("0:0")
        self.assertTrue(unlimited._no_limit)
        self.assertFalse(unlimited.check_limit_reached())

    def test_set_limit_preserves_counters(self):
        self.rate_limit.increase_rate_limit_counter(3)
        prev_counters = {k: v['counter'] for k, v in self.rate_limit._rate_limit_dict.items()}

        self.rate_limit.set_limit("20:2,120:20")
        for key, counter in prev_counters.items():
            if key in self.rate_limit._rate_limit_dict:
                self.assertGreaterEqual(self.rate_limit._rate_limit_dict[key]['counter'], counter)

    def test_get_rate_limits_by_host(self):
        limit, dp_limit = RateLimit.get_rate_limits_by_host(
            "thingsboard.cloud",
            "DEFAULT_TELEMETRY_RATE_LIMIT",
            "DEFAULT_TELEMETRY_DP_RATE_LIMIT"
        )
        self.assertEqual(limit, "10:1,60:60,")
        self.assertEqual(dp_limit, "10:1,300:60,")

    def test_limit_reset_after_time_passes(self):
        self.rate_limit.increase_rate_limit_counter(10)
        self.assertTrue(self.rate_limit.check_limit_reached())
        sleep(1.1)
        self.assertFalse(self.rate_limit.check_limit_reached())

    def test_message_rate_limit(self):
        client = TBDeviceMqttClient("localhost")
        print("Messages rate limit dict:", client._messages_rate_limit._rate_limit_dict)

        if not client._messages_rate_limit._rate_limit_dict:
            client._messages_rate_limit.set_limit("10:1,60:10")

        rate_limit_dict = client._messages_rate_limit._rate_limit_dict
        limit = rate_limit_dict.get(1, {}).get('limit', None)
        if limit is None:
            raise ValueError("Key 1 is missing in the rate limit dict.")

        client._messages_rate_limit.increase_rate_limit_counter(limit + 1)
        print("Messages rate limit after increment:", client._messages_rate_limit._rate_limit_dict)
        self.assertTrue(client._messages_rate_limit.check_limit_reached())
        sleep(1.1)
        self.assertFalse(client._messages_rate_limit.check_limit_reached())

    def test_telemetry_rate_limit(self):
        client = TBDeviceMqttClient("localhost")
        print("Telemetry rate limit dict:", client._telemetry_rate_limit._rate_limit_dict)

        if not client._telemetry_rate_limit._rate_limit_dict:
            client._telemetry_rate_limit.set_limit("10:1,60:10")

        rate_limit_dict = client._telemetry_rate_limit._rate_limit_dict
        limit = rate_limit_dict.get(1, {}).get('limit', None)
        if limit is None:
            raise ValueError("Key 1 is missing in the telemetry rate limit dict.")

        client._telemetry_rate_limit.increase_rate_limit_counter(limit + 1)
        print("Telemetry rate limit after increment:", client._telemetry_rate_limit._rate_limit_dict)
        self.assertTrue(client._telemetry_rate_limit.check_limit_reached())
        sleep(1.1)
        self.assertFalse(client._telemetry_rate_limit.check_limit_reached())

    def test_get_rate_limit_by_host_telemetry_cloud(self):
        result = RateLimit.get_rate_limit_by_host("thingsboard.cloud", "DEFAULT_TELEMETRY_RATE_LIMIT")
        self.assertEqual(result, "10:1,60:60,")

    def test_get_rate_limit_by_host_telemetry_demo(self):
        result = RateLimit.get_rate_limit_by_host("demo.thingsboard.io", "DEFAULT_TELEMETRY_RATE_LIMIT")
        self.assertEqual(result, "10:1,60:60,")

    def test_get_rate_limit_by_host_telemetry_unknown_host(self):
        result = RateLimit.get_rate_limit_by_host("unknown.host", "DEFAULT_TELEMETRY_RATE_LIMIT")
        self.assertEqual(result, "0:0,")

    def test_get_rate_limit_by_host_messages_cloud(self):
        result = RateLimit.get_rate_limit_by_host("thingsboard.cloud", "DEFAULT_MESSAGES_RATE_LIMIT")
        self.assertEqual(result, "10:1,60:60,")

    def test_get_rate_limit_by_host_messages_demo(self):
        result = RateLimit.get_rate_limit_by_host("demo.thingsboard.io", "DEFAULT_MESSAGES_RATE_LIMIT")
        self.assertEqual(result, "10:1,60:60,")

    def test_get_rate_limit_by_host_messages_unknown_host(self):
        result = RateLimit.get_rate_limit_by_host("my.custom.host", "DEFAULT_MESSAGES_RATE_LIMIT")
        self.assertEqual(result, "0:0,")

    def test_get_rate_limit_by_host_custom_string(self):
        result = RateLimit.get_rate_limit_by_host("my.custom.host", "15:2,120:20")
        self.assertEqual(result, "15:2,120:20")

    def test_get_dp_rate_limit_by_host_telemetry_dp_cloud(self):
        result = RateLimit.get_dp_rate_limit_by_host("thingsboard.cloud", "DEFAULT_TELEMETRY_DP_RATE_LIMIT")
        self.assertEqual(result, "10:1,300:60,")

    def test_get_dp_rate_limit_by_host_telemetry_dp_demo(self):
        result = RateLimit.get_dp_rate_limit_by_host("demo.thingsboard.io", "DEFAULT_TELEMETRY_DP_RATE_LIMIT")
        self.assertEqual(result, "10:1,300:60,")

    def test_get_dp_rate_limit_by_host_telemetry_dp_unknown(self):
        result = RateLimit.get_dp_rate_limit_by_host("unknown.host", "DEFAULT_TELEMETRY_DP_RATE_LIMIT")
        self.assertEqual(result, "0:0,")

    def test_get_dp_rate_limit_by_host_custom(self):
        result = RateLimit.get_dp_rate_limit_by_host("my.custom.host", "25:3,80:10,")
        self.assertEqual(result, "25:3,80:10,")

    def test_get_rate_limits_by_topic_with_device(self):
        custom_msg_limit = object()
        custom_dp_limit = object()
        msg_limit, dp_limit = self.client._TBDeviceMqttClient__get_rate_limits_by_topic(
            topic=TELEMETRY_TOPIC,
            device="MyDevice",
            msg_rate_limit=custom_msg_limit,
            dp_rate_limit=custom_dp_limit
        )
        self.assertIs(msg_limit, custom_msg_limit)
        self.assertIs(dp_limit, custom_dp_limit)

    def test_get_rate_limits_by_topic_no_device_telemetry_topic(self):
        msg_limit, dp_limit = self.client._TBDeviceMqttClient__get_rate_limits_by_topic(
            topic=TELEMETRY_TOPIC,
            device=None,
            msg_rate_limit=None,
            dp_rate_limit=None
        )
        self.assertIs(msg_limit, self.client._telemetry_rate_limit)
        self.assertIs(dp_limit, self.client._telemetry_dp_rate_limit)

    def test_get_rate_limits_by_topic_no_device_other_topic(self):
        some_topic = "v1/devices/me/attributes"
        msg_limit, dp_limit = self.client._TBDeviceMqttClient__get_rate_limits_by_topic(
            topic=some_topic,
            device=None,
            msg_rate_limit=None,
            dp_rate_limit=None
        )
        self.assertIs(msg_limit, self.client._messages_rate_limit)
        self.assertIsNone(dp_limit)


class TestOnServiceConfigurationIntegration(unittest.TestCase):
    def setUp(self):
        self.client = TBDeviceMqttClient(
            host="my.test.host",
            port=1883,
            username="fake_token",
            messages_rate_limit="0:0,",
            telemetry_rate_limit="0:0,",
            telemetry_dp_rate_limit="0:0,"
        )
        self.assertIsInstance(self.client._messages_rate_limit, RateLimit)
        self.assertIsInstance(self.client._telemetry_rate_limit, RateLimit)
        self.assertIsInstance(self.client._telemetry_dp_rate_limit, RateLimit)

    def test_on_service_config_error(self):
        config_with_error = {"error": "Some error text"}
        self.client.on_service_configuration(None, config_with_error)
        self.assertTrue(self.client.rate_limits_received, "After ‘error’ rate_limits_received => True")
        self.assertTrue(self.client._messages_rate_limit._no_limit)
        self.assertTrue(self.client._telemetry_rate_limit._no_limit)

    def test_on_service_config_no_rateLimits(self):
        config_no_ratelimits = {"maxInflightMessages": 100}
        self.client.on_service_configuration(None, config_no_ratelimits)
        self.assertTrue(self.client._messages_rate_limit._no_limit)
        self.assertTrue(self.client._telemetry_rate_limit._no_limit)

    def test_on_service_config_partial_rateLimits_no_messages(self):
        config = {
            "rateLimits": {
                "telemetryMessages": "10:1,60:10"
            }
        }
        self.client.on_service_configuration(None, config)
        self.assertFalse(self.client._messages_rate_limit._no_limit)
        self.assertFalse(self.client._telemetry_rate_limit._no_limit)

    def test_on_service_config_all_three(self):
        config = {
            "rateLimits": {
                "messages": "5:1,30:10",
                "telemetryMessages": "10:1,60:20",
                "telemetryDataPoints": "100:10"
            }
        }
        self.client.on_service_configuration(None, config)
        self.assertFalse(self.client._messages_rate_limit._no_limit)
        self.assertFalse(self.client._telemetry_rate_limit._no_limit)
        self.assertFalse(self.client._telemetry_dp_rate_limit._no_limit)

    def test_on_service_config_max_inflight_both_limits(self):
        self.client._messages_rate_limit.set_limit("10:1", 80)
        self.client._telemetry_rate_limit.set_limit("5:1", 80)

        config = {
            "rateLimits": {
                "messages": "10:1",
                "telemetryMessages": "5:1"
            },
            "maxInflightMessages": 50
        }
        self.client.on_service_configuration(None, config)
        self.assertEqual(self.client._client._max_inflight_messages, 3)
        self.assertEqual(self.client._client._max_queued_messages, 3)

    def test_on_service_config_max_inflight_only_messages(self):
        self.client._messages_rate_limit.set_limit("20:1", 80)
        self.client._telemetry_rate_limit.set_limit("0:0,", 80)

        config = {
            "rateLimits": {
                "messages": "20:1"
            },
            "maxInflightMessages": 40
        }
        self.client.on_service_configuration(None, config)
        self.assertEqual(self.client._client._max_inflight_messages, 0)
        self.assertEqual(self.client._client._max_queued_messages, 0)

    def test_on_service_config_max_inflight_only_telemetry(self):
        self.client._messages_rate_limit.set_limit("0:0,", 80)
        self.client._telemetry_rate_limit.set_limit("10:1", 80)

        config = {
            "rateLimits": {
                "telemetryMessages": "10:1"
            },
            "maxInflightMessages": 15
        }
        self.client.on_service_configuration(None, config)
        self.assertEqual(self.client._client._max_inflight_messages, 0)
        self.assertEqual(self.client._client._max_queued_messages, 0)

    def test_on_service_config_max_inflight_no_limits(self):
        self.client._messages_rate_limit.set_limit("0:0,", 80)
        self.client._telemetry_rate_limit.set_limit("0:0,", 80)

        config = {
            "rateLimits": {},
            "maxInflightMessages": 100
        }
        self.client.on_service_configuration(None, config)

        self.assertEqual(self.client._client._max_inflight_messages, 0)
        self.assertEqual(self.client._client._max_queued_messages, 0)

    def test_on_service_config_maxPayloadSize(self):
        config = {
            "rateLimits": {},
            "maxPayloadSize": 2000
        }
        self.client.on_service_configuration(None, config)
        self.assertEqual(self.client.max_payload_size, 1600)


class TestRateLimitParameters(unittest.TestCase):
    def test_default_rate_limits(self):
        client = TBDeviceMqttClient(
            host="fake_host",
            username="dummy",
            password="dummy",
            messages_rate_limit="DEFAULT_MESSAGES_RATE_LIMIT",
            telemetry_rate_limit="DEFAULT_TELEMETRY_RATE_LIMIT",
            telemetry_dp_rate_limit="DEFAULT_TELEMETRY_DP_RATE_LIMIT"
        )
        self.assertTrue(client._messages_rate_limit._no_limit)
        self.assertTrue(client._telemetry_rate_limit._no_limit)
        self.assertTrue(client._telemetry_dp_rate_limit._no_limit)

    def test_custom_rate_limits(self):
        client = TBDeviceMqttClient(
            host="fake_host",
            username="dummy",
            password="dummy",
            messages_rate_limit="20:1,100:60,",
            telemetry_rate_limit="20:1,100:60,",
            telemetry_dp_rate_limit="30:1,200:60,"
        )
        msg_rate_dict = client._messages_rate_limit._rate_limit_dict
        self.assertIn(1, msg_rate_dict)
        self.assertEqual(msg_rate_dict[1]['limit'], 16)
        self.assertIn(60, msg_rate_dict)
        self.assertEqual(msg_rate_dict[60]['limit'], 80)

        telem_rate_dict = client._telemetry_rate_limit._rate_limit_dict
        self.assertIn(1, telem_rate_dict)
        self.assertEqual(telem_rate_dict[1]['limit'], 16)
        self.assertIn(60, telem_rate_dict)
        self.assertEqual(telem_rate_dict[60]['limit'], 80)

        dp_rate_dict = client._telemetry_dp_rate_limit._rate_limit_dict
        self.assertIn(1, dp_rate_dict)
        self.assertEqual(dp_rate_dict[1]['limit'], 24)
        self.assertIn(60, dp_rate_dict)
        self.assertEqual(dp_rate_dict[60]['limit'], 160)


class TestRateLimitFromDict(unittest.TestCase):
    def test_rate_limit_with_rateLimits_key(self):
        rate_limit_input = {
            'rateLimits': {10: {"limit": 100, "counter": 0, "start": 0}},
            'name': 'CustomRate',
            'percentage': 75,
            'no_limit': True
        }
        rl = RateLimit(rate_limit_input)
        self.assertEqual(rl._rate_limit_dict, rate_limit_input['rateLimits'])
        self.assertEqual(rl.name, 'CustomRate')
        self.assertEqual(rl.percentage, 75)
        self.assertTrue(rl._no_limit)

    def test_rate_limit_without_rateLimits_key(self):
        rate_limit_input = {
            10: {"limit": 123, "counter": 0, "start": 0}
        }
        rl = RateLimit(rate_limit_input)
        self.assertEqual(rl._rate_limit_dict, rate_limit_input)
        self.assertIsNone(rl.name)
        self.assertEqual(rl.percentage, 80)
        self.assertFalse(rl._no_limit)


if __name__ == "__main__":
    unittest.main()
