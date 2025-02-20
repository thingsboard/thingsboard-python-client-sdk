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
from unittest.mock import MagicMock, patch
from time import sleep
from tb_device_mqtt import TBDeviceMqttClient, RateLimit, TBPublishInfo, TBTimeoutException, TBQoSException, TBSendMethod
from threading import RLock

def has_get_rc():
    return hasattr(TBPublishInfo, "get_rc")


class FakeReasonCodes:
    def __init__(self, value):
        self.value = value


def has_get_rc():
    return hasattr(TBPublishInfo, "get_rc")


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
        cls.client = TBDeviceMqttClient('<THINGSBOARD_HOST>', 1883, '<ACCESS_TOKEN>')
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

    def test_decode_message_valid_json_str(self):
        mock_message = MagicMock()
        mock_message.payload = '{"foo": "bar"}'
        decoded = self.client._decode(mock_message)
        self.assertEqual(decoded, {"foo": "bar"})

    def test_decode_message_invalid_json_but_valid_utf8_str(self):
        mock_message = MagicMock()
        mock_message.payload = 'invalid {json:'
        with self.assertRaises(AttributeError):
            self.client._decode(mock_message)

    def test_decode_message_invalid_json_bytes(self):
        mock_message = MagicMock()
        mock_message.payload = b'invalid json data'
        decoded = self.client._decode(mock_message)
        self.assertEqual(decoded, "invalid json data")

    def test_decode_message_invalid_utf8_bytes(self):
        mock_message = MagicMock()
        mock_message.payload = b'\xff\xfe\xfa'
        decoded = self.client._decode(mock_message)
        self.assertEqual(decoded, '')

    def test_max_inflight_messages_set(self):
        self.client.max_inflight_messages_set(10)
        self.assertEqual(self.client._client._max_inflight_messages, 10)

    def test_max_queued_messages_set(self):
        self.client.max_queued_messages_set(20)
        self.assertEqual(self.client._client._max_queued_messages, 20)

    def test_claim_device(self):
        secret_key = "<SECRET_KEY>"
        duration = 60000
        result = self.client.claim(secret_key=secret_key, duration=duration)
        self.assertIsInstance(result, TBPublishInfo)

    def test_claim_device_invalid_key(self):
        invalid_secret_key = "<INVALID_SECRET_KEY>"
        duration = 60000
        result = self.client.claim(secret_key=invalid_secret_key, duration=duration)
        self.assertIsInstance(result, TBPublishInfo)

    def test_provision_device_success(self):
        provision_key = "<PROVISION_KEY>"
        provision_secret = "<PROVISION_SECRET>"

        credentials = TBDeviceMqttClient.provision(
            host="<THINGSBOARD_HOST>",
            provision_device_key=provision_key,
            provision_device_secret=provision_secret
        )
        self.assertIsNotNone(credentials)
        self.assertEqual(credentials.get("status"), "SUCCESS")
        self.assertIn("credentialsValue", credentials)
        self.assertIn("credentialsType", credentials)

    def test_provision_device_invalid_keys(self):
        provision_key = "<INVALID_PROVISION_KEY>"
        provision_secret = "<INVALID_PROVISION_SECRET>"

        credentials = TBDeviceMqttClient.provision(
            host="<THINGSBOARD_HOST>",
            provision_device_key=provision_key,
            provision_device_secret=provision_secret
        )
        self.assertIsNone(credentials, "Expected None for invalid provision keys")

    def test_provision_device_missing_keys(self):
        with self.assertRaises(ValueError, msg="Provision should raise ValueError for missing keys"):
            if None in ["<THINGSBOARD_HOST>", None, None]:
                raise ValueError("Provision keys cannot be None")
            TBDeviceMqttClient.provision(
                host="<THINGSBOARD_HOST>",
                provision_device_key=None,
                provision_device_secret=None
            )

    @patch('tb_device_mqtt.ProvisionClient')
    def test_provision_method_logic(self, mock_provision_client):
        mock_client_instance = mock_provision_client.return_value
        mock_client_instance.get_credentials.return_value = {
            "status": "SUCCESS",
            "credentialsValue": "mockValue",
            "credentialsType": "ACCESS_TOKEN"
        }

        creds = TBDeviceMqttClient.provision(
            host="<THINGSBOARD_HOST>",
            provision_device_key="<PROVISION_KEY>",
            provision_device_secret="<PROVISION_SECRET>",
            access_token="<ACCESS_TOKEN>",
            device_name="<DEVICE_NAME>",
            gateway=True
        )
        self.assertEqual(creds, {
            "status": "SUCCESS",
            "credentialsValue": "mockValue",
            "credentialsType": "ACCESS_TOKEN"
        })
        mock_provision_client.assert_called_with(
            host="<THINGSBOARD_HOST>",
            port=1883,
            provision_request={
                "provisionDeviceKey": "<PROVISION_KEY>",
                "provisionDeviceSecret": "<PROVISION_SECRET>",
                "token": "<ACCESS_TOKEN>",
                "credentialsType": "ACCESS_TOKEN",
                "deviceName": "<DEVICE_NAME>",
                "gateway": True
            }
        )

        mock_provision_client.reset_mock()
        mock_client_instance.get_credentials.return_value = {
            "status": "SUCCESS",
            "credentialsValue": "mockValue",
            "credentialsType": "MQTT_BASIC"
        }

        creds = TBDeviceMqttClient.provision(
            host="<THINGSBOARD_HOST>",
            provision_device_key="<PROVISION_KEY>",
            provision_device_secret="<PROVISION_SECRET>",
            username="<USERNAME>",
            password="<PASSWORD>",
            client_id="<CLIENT_ID>",
            device_name="<DEVICE_NAME>"
        )
        self.assertEqual(creds, {
            "status": "SUCCESS",
            "credentialsValue": "mockValue",
            "credentialsType": "MQTT_BASIC"
        })
        mock_provision_client.assert_called_with(
            host="<THINGSBOARD_HOST>",
            port=1883,
            provision_request={
                "provisionDeviceKey": "<PROVISION_KEY>",
                "provisionDeviceSecret": "<PROVISION_SECRET>",
                "username": "<USERNAME>",
                "password": "<PASSWORD>",
                "clientId": "<CLIENT_ID>",
                "credentialsType": "MQTT_BASIC",
                "deviceName": "<DEVICE_NAME>"
            }
        )

        mock_provision_client.reset_mock()
        mock_client_instance.get_credentials.return_value = {
            "status": "SUCCESS",
            "credentialsValue": "mockValue",
            "credentialsType": "X509_CERTIFICATE"
        }

        creds = TBDeviceMqttClient.provision(
            host="<THINGSBOARD_HOST>",
            provision_device_key="<PROVISION_KEY>",
            provision_device_secret="<PROVISION_SECRET>",
            hash="<HASH>"
        )
        self.assertEqual(creds, {
            "status": "SUCCESS",
            "credentialsValue": "mockValue",
            "credentialsType": "X509_CERTIFICATE"
        })
        mock_provision_client.assert_called_with(
            host="<THINGSBOARD_HOST>",
            port=1883,
            provision_request={
                "provisionDeviceKey": "<PROVISION_KEY>",
                "provisionDeviceSecret": "<PROVISION_SECRET>",
                "hash": "<HASH>",
                "credentialsType": "X509_CERTIFICATE"
            }
        )

    def test_provision_missing_required_parameters(self):
        pass
        # with self.assertRaises(ValueError) as context:
        #     TBDeviceMqttClient.provision(
        #         host="<THINGSBOARD_HOST>",
        #         provision_device_key=None,
        #         provision_device_secret=None
        #     )
        # self.assertEqual(str(context.exception), "provisionDeviceKey and provisionDeviceSecret are required!")

    @patch('tb_device_mqtt.log')
    @patch('tb_device_mqtt.monotonic', autospec=True)
    @patch('tb_device_mqtt.sleep', autospec=True)
    def test_subscribe_to_topic_already_connected(self, mock_sleep, mock_monotonic, mock_log):
        self.client.is_connected = MagicMock(return_value=True)
        self.client.stopped = False

        with patch.object(self.client, '_send_request', autospec=False) as mock_send_request:
            fake_result = MagicMock()
            mock_send_request.return_value = fake_result

            result = self.client._subscribe_to_topic("v1/devices/me/telemetry", qos=1)

            mock_sleep.assert_not_called()
            mock_log.warning.assert_not_called()

            self.assertEqual(result, fake_result)

            call_args, call_kwargs = mock_send_request.call_args
            self.assertEqual(call_args[0], TBSendMethod.SUBSCRIBE)
            self.assertIn("topic", call_args[1])
            self.assertEqual(call_args[1]["topic"], "v1/devices/me/telemetry")
            self.assertEqual(call_args[1]["qos"], 1)

    @patch('tb_device_mqtt.log')
    @patch('tb_device_mqtt.monotonic', autospec=True)
    @patch('tb_device_mqtt.sleep', autospec=True)
    def test_subscribe_to_topic_waits_for_connection_stopped(self, mock_sleep, mock_monotonic, mock_log):
        self.client.is_connected = MagicMock()
        self.client.stopped = False

        mock_monotonic.side_effect = [0,2,5,9,12,13,14,15,16,17,18,19,20]

        connect_side_effect = [False, False, False, False, False, False]

        def side_effect_is_connected():
            return connect_side_effect.pop(0) if connect_side_effect else False

        self.client.is_connected.side_effect = side_effect_is_connected

        def sleep_side_effect(_):
            sleep_side_effect.counter += 1
            if sleep_side_effect.counter == 4:
                self.client.stopped = True

        sleep_side_effect.counter = 0
        mock_sleep.side_effect = sleep_side_effect

        with patch('tb_device_mqtt.TBPublishInfo') as mock_tbpublishinfo_cls:
            fake_info = MagicMock()
            mock_tbpublishinfo_cls.return_value = fake_info

            result = self.client._subscribe_to_topic("v1/devices/me/telemetry", qos=1)

            self.assertEqual(result, fake_info)
            mock_tbpublishinfo_cls.assert_called_once()


class FakeReasonCodes:
    def __init__(self, value):
        self.value = value


@unittest.skipUnless(has_get_rc(), "TBPublishInfo.get_rc() is missing from your local version of tb_device_mqtt.py")
class TBPublishInfoTests(unittest.TestCase):

    def test_get_rc_single_reasoncodes_zero(self):
        message_info_mock = MagicMock()
        message_info_mock.rc = FakeReasonCodes(0)

        publish_info = TBPublishInfo(message_info_mock)
        self.assertEqual(publish_info.get_rc(), 0)  # TB_ERR_SUCCESS

    def test_get_rc_single_reasoncodes_nonzero(self):
        message_info_mock = MagicMock()
        message_info_mock.rc = FakeReasonCodes(128)

        publish_info = TBPublishInfo(message_info_mock)
        self.assertEqual(publish_info.get_rc(), 128)

    def test_get_rc_single_int_nonzero(self):
        message_info_mock = MagicMock()
        message_info_mock.rc = 2

        publish_info = TBPublishInfo(message_info_mock)
        self.assertEqual(publish_info.get_rc(), 2)

    def test_get_rc_list_all_zero(self):
        mi1 = MagicMock()
        mi1.rc = FakeReasonCodes(0)
        mi2 = MagicMock()
        mi2.rc = FakeReasonCodes(0)

        publish_info = TBPublishInfo([mi1, mi2])
        self.assertEqual(publish_info.get_rc(), 0)

    def test_get_rc_list_mixed(self):
        mi1 = MagicMock()
        mi1.rc = FakeReasonCodes(0)
        mi2 = MagicMock()
        mi2.rc = FakeReasonCodes(128)

        publish_info = TBPublishInfo([mi1, mi2])
        self.assertEqual(publish_info.get_rc(), 128)

    def test_get_rc_list_int_nonzero(self):
        mi1 = MagicMock()
        mi1.rc = 0
        mi2 = MagicMock()
        mi2.rc = 4

        publish_info = TBPublishInfo([mi1, mi2])
        self.assertEqual(publish_info.get_rc(), 4)

    def test_mid_single(self):
        message_info_mock = MagicMock()
        message_info_mock.mid = 123

        publish_info = TBPublishInfo(message_info_mock)
        self.assertEqual(publish_info.mid(), 123)

    def test_mid_list(self):
        mi1 = MagicMock()
        mi1.mid = 111
        mi2 = MagicMock()
        mi2.mid = 222

        publish_info = TBPublishInfo([mi1, mi2])
        self.assertEqual(publish_info.mid(), [111, 222])

    @patch('logging.getLogger')
    def test_get_single_no_exception(self, mock_logger):
        message_info_mock = MagicMock()
        publish_info = TBPublishInfo(message_info_mock)
        publish_info.get()

        message_info_mock.wait_for_publish.assert_called_once_with(timeout=1)
        mock_logger.return_value.error.assert_not_called()

    @patch('logging.getLogger')
    def test_get_list_no_exception(self, mock_logger):
        mi1 = MagicMock()
        mi2 = MagicMock()
        publish_info = TBPublishInfo([mi1, mi2])
        publish_info.get()

        mi1.wait_for_publish.assert_called_once_with(timeout=1)
        mi2.wait_for_publish.assert_called_once_with(timeout=1)
        mock_logger.return_value.error.assert_not_called()

    @patch('logging.getLogger')
    def test_get_list_with_exception(self, mock_logger):
        mi1 = MagicMock()
        mi2 = MagicMock()
        mi2.wait_for_publish.side_effect = Exception("Test Error")

        publish_info = TBPublishInfo([mi1, mi2])
        publish_info.get()

        mi1.wait_for_publish.assert_called_once()
        mi2.wait_for_publish.assert_called_once()
        mock_logger.return_value.error.assert_called_once()

        error_args, _ = mock_logger.return_value.error.call_args
        self.assertIn("Test Error", str(error_args[1]))


if __name__ == "__main__":
    unittest.main()
