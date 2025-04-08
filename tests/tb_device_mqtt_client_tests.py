# Copyright 2025. ThingsBoard
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
from time import sleep

from tb_device_mqtt import TBDeviceMqttClient


class TBDeviceMqttClientTests(unittest.TestCase):
    """
    Before running tests, do the next steps:
    1. Create device "Example Name" in ThingsBoard
    2. Add shared attribute "attr" with value "hello" to created device
    3. Add client attribute "atr3" with value "value3" to created device
    """

    client = None

    shared_attribute_name = 'attr'
    shared_attribute_value = 'hello'

    client_attribute_name = 'atr3'
    client_attribute_value = 'value3'

    request_attributes_result = None
    subscribe_to_attribute = None
    subscribe_to_attribute_all = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TBDeviceMqttClient('127.0.0.1', 1883, 'TEST_DEVICE_TOKEN')
        cls.client.connect(timeout=1)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.disconnect()

    @staticmethod
    def on_attributes_change_callback(result, exception=None):
        if exception is not None:
            TBDeviceMqttClientTests.request_attributes_result = exception
        else:
            TBDeviceMqttClientTests.request_attributes_result = result

    @staticmethod
    def callback_for_specific_attr(result, *args):
        TBDeviceMqttClientTests.subscribe_to_attribute = result

    @staticmethod
    def callback_for_everything(result, *args):
        TBDeviceMqttClientTests.subscribe_to_attribute_all = result

    def test_request_attributes(self):
        self.client.request_attributes(shared_keys=[self.shared_attribute_name],
                                       callback=self.on_attributes_change_callback)
        sleep(3)
        self.assertEqual(self.request_attributes_result,
                         {'shared': {self.shared_attribute_name: self.shared_attribute_value}})

        self.client.request_attributes(client_keys=[self.client_attribute_name],
                                       callback=self.on_attributes_change_callback)
        sleep(3)
        self.assertEqual(self.request_attributes_result,
                         {'client': {self.client_attribute_name: self.client_attribute_value}})

    def test_send_telemetry_and_attr(self):
        telemetry = {"temperature": 41.9, "humidity": 69, "enabled": False, "currentFirmwareVersion": "v1.2.2"}
        self.assertEqual(self.client.send_telemetry(telemetry, 0).get(), 0)

        attributes = {"sensorModel": "DHT-22", self.client_attribute_name: self.client_attribute_value}
        self.assertEqual(self.client.send_attributes(attributes, 0).get(), 0)

    def test_subscribe_to_attrs(self):
        sub_id_1 = self.client.subscribe_to_attribute(self.shared_attribute_name, self.callback_for_specific_attr)
        sub_id_2 = self.client.subscribe_to_all_attributes(self.callback_for_everything)

        sleep(1)
        value = input("Updated attribute value: ")

        self.assertEqual(self.subscribe_to_attribute_all, {self.shared_attribute_name: value})
        self.assertEqual(self.subscribe_to_attribute, {self.shared_attribute_name: value})

        self.client.unsubscribe_from_attribute(sub_id_1)
        self.client.unsubscribe_from_attribute(sub_id_2)


class TestSplitMessageVariants(unittest.TestCase):

    def test_empty_input(self):
        self.assertEqual(TBDeviceMqttClient._split_message([], 10, 100), [])

    def test_rpc_payload(self):
        rpc_payload = {'device': 'dev1'}
        result = TBDeviceMqttClient._split_message(rpc_payload, 10, 100)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['data'], rpc_payload)

    def test_single_value_message(self):
        msg = [{'ts': 1, 'values': {'a': 1}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertEqual(result[0]['data'][0]['values'], {'a': 1})

    def test_timestamp_change_split(self):
        msg = [{'ts': 1, 'values': {'a': 1}}, {'ts': 2, 'values': {'b': 2}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertEqual(len(result), 2)

    def test_exceeding_datapoint_limit(self):
        msg = [{'ts': 1, 'values': {f'k{i}': i for i in range(10)}}]
        result = TBDeviceMqttClient._split_message(msg, 5, 1000)
        self.assertGreaterEqual(len(result), 2)

    def test_message_with_metadata(self):
        msg = [{'ts': 1, 'values': {'a': 1}, 'metadata': {'unit': 'C'}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertIn('metadata', result[0]['data'][0])

    def test_large_payload_split(self):
        msg = [{'ts': 1, 'values': {f'key{i}': 'v'*50 for i in range(5)}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertGreater(len(result), 1)

    def test_metadata_present_with_ts(self):
        msg = [{'ts': 123456789, 'values': {'temperature': 25}, 'metadata': {'unit': 'C'}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertIn('metadata', result[0]['data'][0])
        self.assertEqual(result[0]['data'][0]['metadata'], {'unit': 'C'})

    def test_metadata_ignored_without_ts(self):
        msg = [{'values': {'temperature': 25}, 'metadata': {'unit': 'C'}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertTrue(all('metadata' not in entry for r in result for entry in r['data']))

    def test_grouping_same_ts_exceeds_datapoint_limit(self):
        msg = [
            {'ts': 1, 'values': {'a': 1, 'b': 2, 'c': 3}},
            {'ts': 1, 'values': {'d': 4, 'e': 5}}
        ]
        result = TBDeviceMqttClient._split_message(msg, datapoints_max_count=3, max_payload_size=1000)
        self.assertGreater(len(result), 1)
        total_keys = set()
        for part in result:
            for d in part['data']:
                total_keys.update(d['values'].keys())
        self.assertEqual(total_keys, {'a', 'b', 'c', 'd', 'e'})

    def test_grouping_same_ts_exceeds_payload_limit(self):
        msg = [
            {'ts': 1, 'values': {'a': 'x'*30}},
            {'ts': 1, 'values': {'b': 'y'*30}},
            {'ts': 1, 'values': {'c': 'z'*30}}
        ]
        result = TBDeviceMqttClient._split_message(msg, datapoints_max_count=10, max_payload_size=64)
        self.assertGreater(len(result), 1)
        all_keys = set()
        for r in result:
            for d in r['data']:
                all_keys.update(d['values'].keys())
        self.assertEqual(all_keys, {'a', 'b', 'c'})

    def test_individual_messages_not_grouped_if_payload_limit_exceeded(self):
        msg = [
            {'ts': 1, 'values': {'a': 'x' * 100}},
            {'ts': 1, 'values': {'b': 'y' * 100}},
            {'ts': 1, 'values': {'c': 'z' * 100}}
        ]
        result = TBDeviceMqttClient._split_message(msg, datapoints_max_count=10, max_payload_size=110)
        self.assertEqual(len(result), 3)

        grouped_keys = []
        for r in result:
            for d in r['data']:
                grouped_keys.extend(d['values'].keys())

        self.assertEqual(set(grouped_keys), {'a', 'b', 'c'})
        for r in result:
            for d in r['data']:
                self.assertLessEqual(sum(len(k) + len(str(v)) for k, v in d['values'].items()), 110)

    def test_partial_grouping_due_to_payload_limit(self):
        msg = [
            {'ts': 1, 'values': {'a': 'x' * 10, 'b': 'y' * 10}},  # should be grouped together
            {'ts': 1, 'values': {'c': 'z' * 100}},  # should be on its own due to size
            {'ts': 1, 'values': {'d': 'w' * 10, 'e': 'q' * 10}}  # should be grouped again
        ]
        result = TBDeviceMqttClient._split_message(msg, datapoints_max_count=10, max_payload_size=64)
        self.assertEqual(len(result), 3)

        result_keys = []
        for r in result:
            keys = []
            for d in r['data']:
                keys.extend(d['values'].keys())
            result_keys.append(set(keys))

        self.assertIn({'a', 'b'}, result_keys)
        self.assertIn({'c'}, result_keys)
        self.assertIn({'d', 'e'}, result_keys)

    def test_partial_grouping_due_to_datapoint_limit(self):
        msg = [
            {'ts': 1, 'values': {'a': 1, 'b': 2}},  # grouped
            {'ts': 1, 'values': {'c': 3, 'd': 4}},  # grouped
            {'ts': 1, 'values': {'e': 5}},  # forced into next group due to datapoint limit
            {'ts': 1, 'values': {'f': 6}},  # grouped with above
        ]
        # Max datapoints per message is 4 (after subtracting 1 in implementation)
        result = TBDeviceMqttClient._split_message(msg, datapoints_max_count=5, max_payload_size=1000)
        self.assertEqual(len(result), 2)

        all_keys = []
        for r in result:
            keys = set()
            for d in r['data']:
                keys.update(d['values'].keys())
            all_keys.append(keys)

        # First group should contain a, b, c, d (4 datapoints)
        self.assertIn({'a', 'b', 'c', 'd'}, all_keys)
        # Second group should contain e, f (2 datapoints)
        self.assertIn({'e', 'f'}, all_keys)

    def test_values_included_only_when_ts_present(self):
        msg = [{'values': {'a': 1, 'b': 2}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertEqual(result[0]['data'][0], {'a': 1, 'b': 2})

    def test_missing_values_field_uses_whole_message(self):
        msg = [{'ts': 123, 'a': 1, 'b': 2}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertIn('values', result[0]['data'][0])
        self.assertEqual(result[0]['data'][0]['values'], {'a': 1, 'b': 2, 'ts': 123})

    def test_metadata_conflict_same_ts_no_grouping(self):
        msg = [
            {'ts': 1, 'values': {'a': 1}, 'metadata': {'unit': 'C'}},
            {'ts': 1, 'values': {'b': 2}, 'metadata': {'unit': 'F'}}
        ]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)

        self.assertEqual(len(result), 2)

        metadata_sets = [d['data'][0].get('metadata') for d in result]
        self.assertIn({'unit': 'C'}, metadata_sets)
        self.assertIn({'unit': 'F'}, metadata_sets)

        value_keys_sets = [set(d['data'][0]['values'].keys()) for d in result]
        self.assertIn({'a'}, value_keys_sets)
        self.assertIn({'b'}, value_keys_sets)

    def test_non_dict_message_is_skipped(self):
        msg = [{'ts': 1, 'values': {'a': 1}}, 'this_is_not_a_dict', {'ts': 1, 'values': {'b': 2}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertEqual(len(result), 1)
        values = result[0]['data'][0]['values']
        self.assertEqual(set(values.keys()), {'a', 'b'})

    def test_multiple_dicts_without_ts_values_metadata(self):
        msg = [{'a': 1}, {'b': 2}, {'c': 3}]
        result = TBDeviceMqttClient._split_message(msg, 10, 1000)
        self.assertEqual(len(result), 1)  # grouped
        combined_keys = {}
        for d in result[0]['data']:
            combined_keys.update(d)
        self.assertEqual(set(combined_keys.keys()), {'a', 'b', 'c'})

    def test_multiple_dicts_without_ts_split_by_payload(self):
        msg = [{'a': 'x' * 60}, {'b': 'y' * 60}, {'c': 'z' * 60}]
        result = TBDeviceMqttClient._split_message(msg, datapoints_max_count=10, max_payload_size=64)
        self.assertEqual(len(result), 3)  # each too large to group
        keys = [list(r['data'][0].keys())[0] for r in result]
        self.assertEqual(set(keys), {'a', 'b', 'c'})

    def test_mixed_dicts_with_and_without_ts(self):
        msg = [
            {'ts': 1, 'values': {'a': 1}},
            {'b': 2},
            {'ts': 1, 'values': {'c': 3}},
            {'d': 4}
        ]
        result = TBDeviceMqttClient._split_message(msg, 10, 1000)
        # Should split into at least 2 chunks: one for ts=1 and one for ts=None
        self.assertGreaterEqual(len(result), 2)

        ts_chunks = [d for r in result for d in r['data'] if 'ts' in d]
        raw_chunks = [d for r in result for d in r['data'] if 'ts' not in d]

        ts_keys = set()
        for d in ts_chunks:
            ts_keys.update(d['values'].keys())

        raw_keys = set()
        for d in raw_chunks:
            raw_keys.update(d.keys())

        self.assertEqual(ts_keys, {'a', 'c'})
        self.assertEqual(raw_keys, {'b', 'd'})

    def test_complex_mixed_messages(self):
        msg = [
            {'ts': 1, 'values': {'a': 1}},
            {'ts': 1, 'values': {'b': 2}, 'metadata': {'unit': 'C'}},

            {'ts': 2, 'values': {'c1': 1, 'c2': 2, 'c3': 3}},
            {'ts': 2, 'values': {'c4': 4, 'c5': 5}},

            {'ts': 3, 'values': {'x': 'x' * 60}},
            {'ts': 3, 'values': {'y': 'y' * 60}},

            {'m1': 1, 'm2': 2},

            123,

            {'m3': 'a' * 100},
            {'m4': 'b' * 100},

            {'k1': 1, 'k2': 2, 'k3': 3},
            {'k4': 4, 'k5': 5},

            {'ts': 1, 'values': {'z': 99}},
        ]

        result = TBDeviceMqttClient._split_message(msg, datapoints_max_count=4, max_payload_size=64)

        all_ts_groups = {}
        raw_chunks = []
        for r in result:
            for entry in r['data']:
                if isinstance(entry, dict) and 'ts' in entry:
                    ts = entry['ts']
                    if ts not in all_ts_groups:
                        all_ts_groups[ts] = set()
                    all_ts_groups[ts].update(entry.get('values', {}).keys())
                    if 'metadata' in entry:
                        self.assertIsInstance(entry['metadata'], dict)
                else:
                    raw_chunks.append(entry)

        self.assertIn(1, all_ts_groups)
        self.assertEqual(all_ts_groups[1], {'a', 'b', 'z'})

        self.assertIn(2, all_ts_groups)
        self.assertEqual(all_ts_groups[2], {'c1', 'c2', 'c3', 'c4', 'c5'})

        self.assertIn(3, all_ts_groups)
        self.assertEqual(all_ts_groups[3], {'x', 'y'})

        all_raw_keys = [set(entry.keys()) for entry in raw_chunks]

        expected_raw_key_sets = [
            {'m1', 'm2'},
            {'m3'},
            {'m4'},
            {'k1', 'k2', 'k3'},
            {'k4', 'k5'}
        ]

        for expected_keys in expected_raw_key_sets:
            self.assertIn(expected_keys, all_raw_keys)

        for r in raw_chunks:
            self.assertLessEqual(len(r), 4)  # Max datapoints = 4

            total_size = sum(len(k) + len(str(v)) for k, v in r.items())

            if len(r) > 1:
                self.assertLessEqual(total_size, 64)
            if total_size > 64:
                self.assertEqual(len(r), 1)

        self.assertGreaterEqual(len(result), 8)

    def test_empty_values_should_skip_or_include_empty(self):
        msg = [{'ts': 1, 'values': {}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertEqual(result, [])

    def test_duplicate_keys_within_same_ts(self):
        msg = [{'ts': 1, 'values': {'a': 1}}, {'ts': 1, 'values': {'a': 2}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        values = {}
        for d in result[0]['data']:
            values.update(d['values'])
        self.assertEqual(values['a'], 2)  # Last value wins

    def test_partial_metadata_presence(self):
        msg = [{'ts': 1, 'values': {'a': 1}, 'metadata': {'unit': 'C'}},
               {'ts': 1, 'values': {'b': 2}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        for d in result[0]['data']:
            if d['values'].keys() == {'a', 'b'} or d['values'].keys() == {'b', 'a'}:
                self.assertIn('metadata', d)

    def test_non_dict_metadata_should_be_ignored(self):
        msg = [{'ts': 1, 'values': {'a': 1}, 'metadata': ['invalid']}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertTrue(all(
            isinstance(d.get('metadata', {}), dict) or 'metadata' not in d
            for r in result for d in r['data']
        ))

    def test_non_list_message_pack_single_dict_raw(self):
        msg = {'a': 1, 'b': 2}
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        self.assertEqual(result[0]['data'][0], msg)

    def test_nested_value_object_should_count_size_correctly(self):
        msg = [{'ts': 1, 'values': {'a': {'nested': 'structure'}}}]
        result = TBDeviceMqttClient._split_message(msg, 10, 1000)
        total_size = sum(len(k) + len(str(v)) for r in result for d in r['data'] for k, v in d['values'].items())
        self.assertGreater(total_size, 0)

    def test_raw_duplicate_keys_overwrite_behavior(self):
        msg = [{'a': 1}, {'a': 2}]
        result = TBDeviceMqttClient._split_message(msg, 10, 100)
        all_data = {}
        for r in result:
            for d in r['data']:
                all_data.update(d)
        self.assertEqual(all_data['a'], 2)  # Last value wins


if __name__ == '__main__':
    unittest.main('tb_device_mqtt_client_tests')
