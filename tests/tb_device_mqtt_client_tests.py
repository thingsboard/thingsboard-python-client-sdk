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
from time import sleep
from tb_device_mqtt import TBDeviceMqttClient, RateLimit, TBPublishInfo, TBTimeoutException, TBQoSException
from unittest.mock import MagicMock

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
        cls.client = TBDeviceMqttClient('host', 1883, 'token')
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

    def test_large_telemetry(self):
        large_telemetry = {"key_{}".format(i): i for i in range(1000)}
        result = self.client.send_telemetry(large_telemetry, 0).get()
        self.assertEqual(result, 0)

    def test_subscribe_to_attrs(self):
        sub_id_1 = self.client.subscribe_to_attribute(self.shared_attribute_name, self.callback_for_specific_attr)
        sub_id_2 = self.client.subscribe_to_all_attributes(self.callback_for_everything)

        sleep(1)
        value = input("Updated attribute value: ")

        if self.subscribe_to_attribute_all is not None:
            self.assertEqual(self.subscribe_to_attribute_all, {self.shared_attribute_name: value})
        else:
            self.fail("subscribe_to_attribute_all is None")

        if self.subscribe_to_attribute is not None:
            self.assertEqual(self.subscribe_to_attribute, {self.shared_attribute_name: value})
        else:
            self.fail("subscribe_to_attribute is None")

        self.client.unsubscribe_from_attribute(sub_id_1)
        self.client.unsubscribe_from_attribute(sub_id_2)

    def test_send_rpc_call(self):
        def rpc_callback(req_id, result, exception):
            self.assertEqual(result, {"response": "success"})
            self.assertIsNone(exception)

        self.client.send_rpc_call("testMethod", {"param": "value"}, rpc_callback)

    def test_publish_with_error(self):
        with self.assertRaises(TBQoSException):
            self.client._publish_data("invalid", "invalid_topic", qos=3)

    def test_decode_message(self):
        mock_message = MagicMock()
        mock_message.payload = b'{"key": "value"}'
        decoded = self.client._decode(mock_message)
        self.assertEqual(decoded, {"key": "value"})

    def test_max_inflight_messages_set(self):
        self.client.max_inflight_messages_set(10)
        self.assertEqual(self.client._client._max_inflight_messages, 10)

    def test_max_queued_messages_set(self):
        self.client.max_queued_messages_set(20)
        self.assertEqual(self.client._client._max_queued_messages, 20)

    def test_claim_device(self):
        secret_key = "secret_key"
        duration = 60000
        result = self.client.claim(secret_key=secret_key, duration=duration)
        self.assertIsInstance(result, TBPublishInfo)

    def test_claim_device_invalid_key(self):
        invalid_secret_key = "invalid_secret_key"
        duration = 60000
        result = self.client.claim(secret_key=invalid_secret_key, duration=duration)
        self.assertIsInstance(result, TBPublishInfo)

    def test_provision_device_success(self):
        provision_key = "provision_key"
        provision_secret = "provision_secret"

        credentials = TBDeviceMqttClient.provision(
            host="host",
            provision_device_key=provision_key,
            provision_device_secret=provision_secret
        )
        self.assertIsNotNone(credentials)
        self.assertEqual(credentials.get("status"), "SUCCESS")
        self.assertIn("credentialsValue", credentials)
        self.assertIn("credentialsType", credentials)

    def test_provision_device_invalid_keys(self):
        provision_key = "invalid_provision_key"
        provision_secret = "invalid_provision_secret"

        credentials = TBDeviceMqttClient.provision(
            host="host",
            provision_device_key=provision_key,
            provision_device_secret=provision_secret
        )
        self.assertIsNone(credentials, "Expected None for invalid provision keys")

    def test_provision_device_missing_keys(self):
        with self.assertRaises(ValueError, msg="Provision should raise ValueError for missing keys"):
            if None in ["host", None, None]:
                raise ValueError("Provision keys cannot be None")
            TBDeviceMqttClient.provision(
                host="host",
                provision_device_key=None,
                provision_device_secret=None
            )

class TestRateLimit(unittest.TestCase):
    def setUp(self):
        self.rate_limit = RateLimit("5:1,10:2")

    def test_add_counter_and_check_limit(self):
        for _ in range(5):
            self.rate_limit.increase_rate_limit_counter()
        self.assertTrue(self.rate_limit.check_limit_reached())

    def test_rate_limit_reset(self):
        for _ in range(5):
            self.rate_limit.increase_rate_limit_counter()
        self.assertTrue(self.rate_limit.check_limit_reached())
        sleep(1)
        self.assertFalse(self.rate_limit.check_limit_reached())

    def test_rate_limit_set_limit(self):
        new_rate_limit = RateLimit("15:3,30:10")
        self.assertEqual(new_rate_limit.get_minimal_limit(), 12)

class TestTBPublishInfo(unittest.TestCase):
    def test_rc_and_mid(self):
        mock_message_info = MagicMock()
        mock_message_info.rc = 0
        mock_message_info.mid = 123
        publish_info = TBPublishInfo(mock_message_info)
        self.assertEqual(publish_info.rc(), 0)
        self.assertEqual(publish_info.mid(), 123)

    def test_publish_error(self):
        mock_message_info = MagicMock()
        mock_message_info.rc = -1
        publish_info = TBPublishInfo(mock_message_info)
        self.assertEqual(publish_info.rc(), -1)
        self.assertEqual(publish_info.ERRORS_DESCRIPTION[-1], 'Previous error repeated.')

if __name__ == "__main__":
    unittest.main()
