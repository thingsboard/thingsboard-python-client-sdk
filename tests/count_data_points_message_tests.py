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
from tb_device_mqtt import TBDeviceMqttClient


class TestCountDataPointsInMessage(unittest.TestCase):

    def test_simple_dict_no_device(self):
        data = {
            "ts": 123456789,
            "values": {
                "temp": 22.5,
                "humidity": 55
            }
        }
        result = TBDeviceMqttClient._count_datapoints_in_message(data)
        self.assertEqual(result, 2)

    def test_list_of_dict_no_device(self):
        data = [
            {"ts": 123456789, "values": {"temp": 22.5, "humidity": 55}},
            {"ts": 123456799, "values": {"light": 100, "pressure": 760}}
        ]
        result = TBDeviceMqttClient._count_datapoints_in_message(data)
        self.assertEqual(result, 4)

    def test_with_device_dict_inside(self):
        data = {
            "MyDevice": {
                "ts": 123456789,
                "values": {"temp": 22.5, "humidity": 55}
            },
            "OtherKey": "some_value"
        }
        result = TBDeviceMqttClient._count_datapoints_in_message(data, device="MyDevice")
        self.assertEqual(result, 2)

    def test_with_device_list_inside(self):
        data = {
            "Sensor": [
                {"ts": 1, "values": {"v1": 10}},
                {"ts": 2, "values": {"v2": 20, "v3": 30}}
            ]
        }
        result = TBDeviceMqttClient._count_datapoints_in_message(data, device="Sensor")
        self.assertEqual(result, 3)

    def test_empty_dict_no_device(self):
        data = {}
        result = TBDeviceMqttClient._count_datapoints_in_message(data)
        self.assertEqual(result, 0)

    def test_missing_device_key(self):

        data = {"some_unrelated_key": 42}
        result = TBDeviceMqttClient._count_datapoints_in_message(data, device="NotExistingDeviceKey")
        self.assertEqual(result, 1)

    def test_data_is_string_no_device(self):
        data = "just a string"
        result = TBDeviceMqttClient._count_datapoints_in_message(data)
        self.assertEqual(result, 1)


if __name__ == '__main__':
    unittest.main()
