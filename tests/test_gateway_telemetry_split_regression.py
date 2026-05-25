# Copyright 2026. ThingsBoard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from tb_device_mqtt import TBDeviceMqttClient


class GatewayTelemetrySplitRegressionTest(unittest.TestCase):

    def test_unordered_gateway_telemetry_keeps_original_timestamps(self):
        telemetry = [
            {
                'ts': 1736330010000,
                'values': {
                    'test_1': 1,
                    'test_1_1': 1,
                }
            },
            {
                'ts': 1736330030000,
                'values': {
                    'test_3': 3,
                    'test_3_1': 3,
                }
            },
            {
                'ts': 1736330020000,
                'values': {
                    'test_2': 2,
                    'test_2_1': 2
                }
            },
        ]

        self.assert_split_preserves_telemetry(telemetry)

    def test_ordered_gateway_telemetry_with_uneven_values_keeps_all_datapoints(self):
        telemetry = [
            {
                'ts': 1736330010000,
                'values': {
                    'test_1': 1,
                    'test_1_1': 1
                }
            },
            {
                'ts': 1736330020000,
                'values': {
                    'test_2': 2,
                    'test_2_1': 2
                }
            },
            {
                'ts': 1736330030000,
                'values': {
                    'test_3': 3
                }
            },
        ]

        self.assert_split_preserves_telemetry(telemetry)

    def test_unordered_gateway_telemetry_with_uneven_values_keeps_all_datapoints(self):
        telemetry = [
            {
                'ts': 1736330010000,
                'values': {
                    'test_1': 1,
                    'test_1_1': 1,
                }
            },
            {
                'ts': 1736330030000,
                'values': {
                    'test_3': 3,
                }
            },
            {
                'ts': 1736330020000,
                'values': {
                    'test_2': 2,
                    'test_2_1': 2
                }
            },
        ]

        self.assert_split_preserves_telemetry(telemetry)

    def assert_split_preserves_telemetry(self, telemetry):
        expected = self.flatten_input(telemetry)

        for datapoints_limit in (0, 1, 2):
            with self.subTest(datapoints_limit=datapoints_limit):
                split_messages = TBDeviceMqttClient._split_message(
                    telemetry, datapoints_limit, max_payload_size=8196)

                self.assertEqual(self.flatten_split_messages(split_messages), expected)

    @staticmethod
    def flatten_input(telemetry):
        result = {}
        for item in telemetry:
            for key, value in item['values'].items():
                result[key] = (item['ts'], value)
        return result

    @staticmethod
    def flatten_split_messages(split_messages):
        result = {}
        for split_message in split_messages:
            for item in split_message['data']:
                for key, value in item['values'].items():
                    if key in result:
                        raise AssertionError(f'Duplicate telemetry key: {key}')
                    result[key] = (item['ts'], value)
        return result


if __name__ == '__main__':
    unittest.main()
