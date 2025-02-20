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
from unittest.mock import MagicMock,patch
from time import sleep, time
import threading
from tb_gateway_mqtt import TBGatewayMqttClient, TBSendMethod, GATEWAY_CLAIMING_TOPIC, GATEWAY_RPC_TOPIC, GATEWAY_MAIN_TOPIC


class TestGwUnsubscribe(unittest.TestCase):
    def setUp(self):
        self.client = TBGatewayMqttClient("localhost", 1883, "dummy_token")
        if not hasattr(self.client, "_lock"):
            self.client._lock = threading.Lock()
        self.client._TBGatewayMqttClient__sub_dict = {
            "device1|attr1": {1: lambda msg: "callback1"},
            "device2|attr2": {2: lambda msg: "callback2"},
        }

    def test_unsubscribe_specific(self):
        sub_dict = self.client._TBGatewayMqttClient__sub_dict
        self.assertIn(1, sub_dict["device1|attr1"])
        self.assertIn(2, sub_dict["device2|attr2"])

        self.client.gw_unsubscribe(1)

        sub_dict = self.client._TBGatewayMqttClient__sub_dict
        self.assertNotIn(1, sub_dict["device1|attr1"])
        self.assertIn(2, sub_dict["device2|attr2"])

    def test_unsubscribe_all(self):
        self.client.gw_unsubscribe('*')
        self.assertEqual(self.client._TBGatewayMqttClient__sub_dict, {})

class TestGwSendRpcReply(unittest.TestCase):
    def setUp(self):
        self.client = TBGatewayMqttClient("localhost", 1883, "dummy_token")
        if not hasattr(self.client, "_lock"):
            self.client._lock = threading.Lock()

    def test_gw_send_rpc_reply_default_qos(self):
        device = "test_device"
        req_id = 101
        resp = {"status": "ok"}
        self.client.quality_of_service = 1
        dummy_info = "info_default_qos"

        def fake_send_device_request(method, device_arg, topic, data, qos):
            self.assertEqual(method, TBSendMethod.PUBLISH)
            self.assertEqual(device_arg, device)
            self.assertEqual(topic, GATEWAY_RPC_TOPIC)
            self.assertEqual(data, {"device": device, "id": req_id, "data": resp})
            self.assertEqual(qos, 1)
            return dummy_info

        self.client._send_device_request = fake_send_device_request
        result = self.client.gw_send_rpc_reply(device, req_id, resp)
        self.assertEqual(result, dummy_info)

    def test_gw_send_rpc_reply_explicit_valid_qos(self):
        device = "test_device"
        req_id = 202
        resp = {"status": "success"}
        explicit_qos = 0
        dummy_info = "info_explicit_qos"

        def fake_send_device_request(method, device_arg, topic, data, qos):
            self.assertEqual(method, TBSendMethod.PUBLISH)
            self.assertEqual(device_arg, device)
            self.assertEqual(topic, GATEWAY_RPC_TOPIC)
            self.assertEqual(data, {"device": device, "id": req_id, "data": resp})
            self.assertEqual(qos, explicit_qos)
            return dummy_info

        self.client._send_device_request = fake_send_device_request
        result = self.client.gw_send_rpc_reply(device, req_id, resp, quality_of_service=explicit_qos)
        self.assertEqual(result, dummy_info)

    def test_gw_send_rpc_reply_invalid_qos(self):
        device = "test_device"
        req_id = 303
        resp = {"status": "fail"}
        invalid_qos = 2
        self.client.quality_of_service = 1

        result = self.client.gw_send_rpc_reply(device, req_id, resp, quality_of_service=invalid_qos)
        self.assertIsNone(result)

class TestOnServiceConfiguration(unittest.TestCase):
    def setUp(self):
        self.client = TBGatewayMqttClient("localhost", 1883, "dummy_token")
        if not hasattr(self.client, "_lock"):
            self.client._lock = threading.Lock()
        self.client._devices_connected_through_gateway_messages_rate_limit = MagicMock()
        self.client._devices_connected_through_gateway_telemetry_messages_rate_limit = MagicMock()
        self.client._devices_connected_through_gateway_telemetry_datapoints_rate_limit = MagicMock()
        self.client.rate_limits_received = False

    def test_on_service_configuration_error(self):
        error_response = {"error": "timeout"}
        parent_class = self.client.__class__.__bases__[0]
        with patch.object(parent_class, "on_service_configuration") as mock_parent_on_service_configuration:
            self.client._TBGatewayMqttClient__on_service_configuration("dummy_arg", error_response)
            self.assertTrue(self.client.rate_limits_received)
            mock_parent_on_service_configuration.assert_not_called()

    def test_on_service_configuration_valid(self):
        response = {
            "gatewayRateLimits": {
                "messages": "10:20",
                "telemetryMessages": "30:40",
                "telemetryDataPoints": "50:60",
            },
            "rateLimits": {"limit": "value"},
            "other_config": "other_value"
        }
        response_copy = response.copy()
        parent_class = self.client.__class__.__bases__[0]
        with patch.object(parent_class, "on_service_configuration") as mock_parent_on_service_configuration:
            self.client._TBGatewayMqttClient__on_service_configuration("dummy_arg", response_copy, "extra_arg", key="extra")
            self.client._devices_connected_through_gateway_messages_rate_limit.set_limit.assert_called_with("10:20")
            self.client._devices_connected_through_gateway_telemetry_messages_rate_limit.set_limit.assert_called_with("30:40")
            self.client._devices_connected_through_gateway_telemetry_datapoints_rate_limit.set_limit.assert_called_with("50:60")
            expected_dict = {'rateLimit': {"limit": "value"}, "other_config": "other_value"}
            mock_parent_on_service_configuration.assert_called_with("dummy_arg", expected_dict, "extra_arg", key="extra")

    def test_on_service_configuration_default_telemetry_datapoints(self):
        response = {
            "gatewayRateLimits": {
                "messages": "10:20",
                "telemetryMessages": "30:40",
            },
            "rateLimits": {"limit": "value"},
            "other_config": "other_value"
        }
        response_copy = response.copy()
        parent_class = self.client.__class__.__bases__[0]
        with patch.object(parent_class, "on_service_configuration") as mock_parent_on_service_configuration:
            self.client._TBGatewayMqttClient__on_service_configuration("dummy_arg", response_copy, "extra_arg", key="extra")
            self.client._devices_connected_through_gateway_telemetry_datapoints_rate_limit.set_limit.assert_called_with("0:0,")
            expected_dict = {'rateLimit': {"limit": "value"}, "other_config": "other_value"}
            mock_parent_on_service_configuration.assert_called_with("dummy_arg", expected_dict, "extra_arg", key="extra")

class TestGwDisconnectDevice(unittest.TestCase):
    def setUp(self):
        self.client = TBGatewayMqttClient("localhost", 1883, "dummy_token")
        if not hasattr(self.client, "_lock"):
            self.client._lock = threading.Lock()
        self.client._TBGatewayMqttClient__connected_devices = {"test_device", "another_device"}

    def test_disconnect_existing_device(self):
        device = "test_device"
        dummy_info = "disconnect_info"

        def fake_send_device_request(method, device_arg, topic, data, qos):
            self.assertEqual(method, TBSendMethod.PUBLISH)
            self.assertEqual(device_arg, device)
            self.assertEqual(topic, GATEWAY_MAIN_TOPIC + "disconnect")
            self.assertEqual(data, {"device": device})
            self.assertEqual(qos, self.client.quality_of_service)
            return dummy_info

        self.client._send_device_request = fake_send_device_request
        self.client.quality_of_service = 1
        self.assertIn(device, self.client._TBGatewayMqttClient__connected_devices)
        result = self.client.gw_disconnect_device(device)
        self.assertEqual(result, dummy_info)
        self.assertNotIn(device, self.client._TBGatewayMqttClient__connected_devices)

    def test_disconnect_non_existing_device(self):
        device = "non_existing_device"
        dummy_info = "disconnect_info_non_existing"

        def fake_send_device_request(method, device_arg, topic, data, qos):
            self.assertEqual(method, TBSendMethod.PUBLISH)
            self.assertEqual(device_arg, device)
            self.assertEqual(topic, GATEWAY_MAIN_TOPIC + "disconnect")
            self.assertEqual(data, {"device": device})
            self.assertEqual(qos, self.client.quality_of_service)
            return dummy_info

        self.client._send_device_request = fake_send_device_request
        self.client.quality_of_service = 1
        self.assertNotIn(device, self.client._TBGatewayMqttClient__connected_devices)
        result = self.client.gw_disconnect_device(device)
        self.assertEqual(result, dummy_info)

class TestOtherFunctions(unittest.TestCase):
    def setUp(self):
        self.client = TBGatewayMqttClient("localhost", 1883, "dummy_token")
        self.client._gw_subscriptions = {}

    def test_delete_subscription(self):
        self.client._gw_subscriptions = {42: "dummy_subscription"}
        topic = "some_topic"
        subscription_id = 42

        self.client._delete_subscription(topic, subscription_id)

        self.assertNotIn(subscription_id, self.client._gw_subscriptions)

    def test_get_subscriptions_in_progress(self):
        self.client._gw_subscriptions = {}
        self.assertFalse(self.client.get_subscriptions_in_progress())

        self.client._gw_subscriptions = {1: "dummy_subscription"}
        self.assertTrue(self.client.get_subscriptions_in_progress())

    def test_gw_request_client_attributes(self):
        def fake_request_attributes(device, keys, callback, type_is_client):
            self.fake_request_called = True
            self.request_args = (device, keys, callback, type_is_client)
            return "fake_result"

        self.client._TBGatewayMqttClient__request_attributes = fake_request_attributes

        device_name = "test_device"
        keys = ["attr1", "attr2"]

        def dummy_callback(response, error):
            pass

        result = self.client.gw_request_client_attributes(device_name, keys, dummy_callback)

        self.assertTrue(hasattr(self, "fake_request_called"))
        self.assertTrue(self.fake_request_called)
        self.assertEqual(self.request_args, (device_name, keys, dummy_callback, True))
        self.assertEqual(result, "fake_result")

    def test_gw_set_server_side_rpc_request_handler(self):
        def dummy_handler(client, request):
            pass

        self.client.gw_set_server_side_rpc_request_handler(dummy_handler)
        self.assertEqual(self.client.devices_server_side_rpc_request_handler, dummy_handler)

class TestGwClaim(unittest.TestCase):
    def setUp(self):
        self.client = TBGatewayMqttClient("localhost", 1883, "dummy_token")
        self.client.quality_of_service = 1
        self.client._send_device_request = MagicMock()

    def test_gw_claim_default(self):
        device_name = "device1"
        secret_key = "mySecret"
        duration = 30000
        dummy_info = "claim_info"
        self.client._send_device_request.return_value = dummy_info

        result = self.client.gw_claim(device_name, secret_key, duration)

        expected_claiming_request = {
            device_name: {
                "secretKey": secret_key,
                "durationMs": duration
            }
        }
        self.client._send_device_request.assert_called_once_with(
            TBSendMethod.PUBLISH,
            device_name,
            topic=GATEWAY_CLAIMING_TOPIC,
            data=expected_claiming_request,
            qos=self.client.quality_of_service
        )
        self.assertEqual(result, dummy_info)

    def test_gw_claim_custom(self):
        device_name = "device2"
        secret_key = "otherSecret"
        duration = 60000
        custom_claim = {"custom": "value"}
        dummy_info = "custom_claim_info"
        self.client._send_device_request.return_value = dummy_info

        result = self.client.gw_claim(device_name, secret_key, duration, claiming_request=custom_claim)

        self.client._send_device_request.assert_called_once_with(
            TBSendMethod.PUBLISH,
            device_name,
            topic=GATEWAY_CLAIMING_TOPIC,
            data=custom_claim,
            qos=self.client.quality_of_service
        )
        self.assertEqual(result, dummy_info)

class TBGatewayMqttClientTests(unittest.TestCase):
    """
    Before running tests, do the next steps:
    1. Create device "Example Name" in ThingsBoard
    2. Add shared attribute "attr" with value "hello" to created device
    """

    client = None

    device_name = 'Example Name'
    shared_attr_name = 'attr'
    shared_attr_value = 'hello'

    request_attributes_result = None
    subscribe_to_attribute = None
    subscribe_to_attribute_all = None
    subscribe_to_device_attribute_all = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TBGatewayMqttClient('<HOST>', 1883, '<ACCSESS TOKEN>')
        cls.client.connect(timeout=1)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.disconnect()

    @staticmethod
    def request_attributes_callback(result, exception=None):
        if exception is not None:
            TBGatewayMqttClientTests.request_attributes_result = exception
        else:
            TBGatewayMqttClientTests.request_attributes_result = result

    @staticmethod
    def callback(result):
        TBGatewayMqttClientTests.subscribe_to_device_attribute_all = result

    @staticmethod
    def callback_for_everything(result):
        TBGatewayMqttClientTests.subscribe_to_attribute_all = result

    @staticmethod
    def callback_for_specific_attr(result):
        TBGatewayMqttClientTests.subscribe_to_attribute = result

    def test_connect_disconnect_device(self):
        self.assertEqual(self.client.gw_connect_device(self.device_name).rc, 0)
        self.assertEqual(self.client.gw_disconnect_device(self.device_name).rc, 0)

    def test_request_attributes(self):
        self.client.gw_request_shared_attributes(self.device_name, [self.shared_attr_name],
                                                 self.request_attributes_callback)
        sleep(3)
        self.assertEqual(self.request_attributes_result,
                         {'id': 1, 'device': self.device_name, 'value': self.shared_attr_value})

    def test_send_telemetry_and_attributes(self):
        attributes = {"atr1": 1, "atr2": True, "atr3": "value3"}
        telemetry = {"ts": int(round(time() * 1000)), "values": {"key1": "11"}}
        self.assertEqual(self.client.gw_send_attributes(self.device_name, attributes).get(), 0)
        self.assertEqual(self.client.gw_send_telemetry(self.device_name, telemetry).get(), 0)

    def test_subscribe_to_attributes(self):
        self.client.gw_connect_device(self.device_name)

        self.client.gw_subscribe_to_all_attributes(self.callback_for_everything)
        self.client.gw_subscribe_to_attribute(self.device_name, self.shared_attr_name, self.callback_for_specific_attr)
        sub_id = self.client.gw_subscribe_to_all_device_attributes(self.device_name, self.callback)

        sleep(1)
        value = input("Updated attribute value: ")

        self.assertEqual(self.subscribe_to_attribute,
                         {'device': self.device_name, 'data': {self.shared_attr_name: value}})
        self.assertEqual(self.subscribe_to_attribute_all,
                         {'device': self.device_name, 'data': {self.shared_attr_name: value}})
        self.assertEqual(self.subscribe_to_device_attribute_all,
                         {'device': self.device_name, 'data': {self.shared_attr_name: value}})

        self.client.gw_unsubscribe(sub_id)


if __name__ == '__main__':
    unittest.main('tb_gateway_mqtt_client_tests')
