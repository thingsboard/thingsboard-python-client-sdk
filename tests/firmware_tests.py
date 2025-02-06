import unittest
from unittest.mock import MagicMock, patch
from tb_device_mqtt import TBDeviceMqttClient, TBTimeoutException


class TestTBDeviceMqttClient(unittest.TestCase):

    @patch('tb_device_mqtt.paho.Client')
    def setUp(self, mock_paho_client):
        self.mock_mqtt_client = mock_paho_client.return_value
        self.client = TBDeviceMqttClient(host='thingsboard.cloud', port=1883, username='gEVBWSkNkLR8VmkHz9F0',
                                         password=None)

    def test_connect(self):
        self.client.connect()
        self.mock_mqtt_client.connect.assert_called_with('thingsboard.cloud', 1883, keepalive=120)
        self.mock_mqtt_client.loop_start.assert_called()

    def test_disconnect(self):
        self.client.disconnect()
        self.mock_mqtt_client.disconnect.assert_called()
        self.mock_mqtt_client.loop_stop.assert_called()

    def test_send_telemetry(self):
        self.client._publish_data = MagicMock()
        self.client.send_telemetry({'temp': 22})
        self.client._publish_data.assert_called_with([{'temp': 22}], 'v1/devices/me/telemetry', 1, True)

    def test_get_firmware_update(self):
        self.client._client.subscribe = MagicMock()
        self.client.send_telemetry = MagicMock()
        self.client._publish_data = MagicMock()

        self.client.get_firmware_update()

        self.client._client.subscribe.assert_called_with('v2/fw/response/+')
        self.client.send_telemetry.assert_called()
        self.client._publish_data.assert_called()

    def test_firmware_download_process(self):
        self.client._publish_data = MagicMock()

        self.client.firmware_info = {
            "fw_title": "NewFirmware",
            "fw_version": "2.0",
            "fw_size": 1024,
            "fw_checksum": "abc123",
            "fw_checksum_algorithm": "SHA256"
        }

        self.client._TBDeviceMqttClient__current_chunk = 0
        self.client._TBDeviceMqttClient__firmware_request_id = 1

        self.client._TBDeviceMqttClient__get_firmware()

        self.client._publish_data.assert_called()

    def test_firmware_verification_success(self):
        self.client._publish_data = MagicMock()
        self.client.firmware_data = b'binary data'
        self.client.firmware_info = {
            "fw_title": "NewFirmware",
            "fw_version": "2.0",
            "fw_checksum": "valid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }

        self.client._TBDeviceMqttClient__process_firmware()

        self.client._publish_data.assert_called()

    def test_firmware_verification_failure(self):
        self.client._publish_data = MagicMock()
        self.client.firmware_data = b'corrupt data'
        self.client.firmware_info = {
            "fw_title": "NewFirmware",
            "fw_version": "2.0",
            "fw_checksum": "invalid_checksum",
            "fw_checksum_algorithm": "SHA256"
        }

        self.client._TBDeviceMqttClient__process_firmware()

        self.client._publish_data.assert_called()

    def test_firmware_state_transition(self):
        self.client._publish_data = MagicMock()
        self.client.current_firmware_info = {
            "current_fw_title": "OldFirmware",
            "current_fw_version": "1.0",
            "fw_state": "IDLE"
        }

        self.client.firmware_received = True
        self.client._TBDeviceMqttClient__service_loop()
        self.client._publish_data.assert_called()

    def test_firmware_request_info(self):
        self.client._publish_data = MagicMock()
        self.client._TBDeviceMqttClient__request_firmware_info()

        self.client._publish_data.assert_called()

    def test_firmware_chunk_reception(self):
        self.client._publish_data = MagicMock()
        self.client._TBDeviceMqttClient__get_firmware()

        self.client._publish_data.assert_called()

    def test_timeout_exception(self):
        with self.assertRaises(TBTimeoutException):
            raise TBTimeoutException("Timeout occurred")
# вот такой юнит тест и мне надо чтобы ты помог мне решить проблему с тем что у меня вылазит такая ошибка данный юнит тест проверяет вот этот файл

if __name__ == '__main__':
    unittest.main()
