import unittest
from unittest.mock import patch, MagicMock
from tb_gateway_mqtt import TBGatewayMqttClient
from tb_device_mqtt import TBDeviceMqttClient


class TestRateLimitInitialization(unittest.TestCase):
    @staticmethod
    def fake_init(instance, host, port, username, password, quality_of_service, client_id, **kwargs):
        instance._init_kwargs = kwargs
        instance._client = MagicMock()

    def test_custom_rate_limits(self):
        custom_rate = "MY_RATE_LIMIT"
        custom_dp = "MY_RATE_LIMIT_DP"

        with patch("tb_gateway_mqtt.RateLimit.__init__", return_value=None), \
                patch("tb_gateway_mqtt.RateLimit.get_rate_limits_by_host", return_value=(custom_rate, custom_dp)), \
                patch("tb_gateway_mqtt.RateLimit.get_rate_limit_by_host", return_value=custom_rate), \
                patch.object(TBDeviceMqttClient, '__init__', new=TestRateLimitInitialization.fake_init):
            client = TBGatewayMqttClient(
                host="localhost",
                port=1883,
                username="dummy_token",
                rate_limit=custom_rate,
                dp_rate_limit=custom_dp
            )
            captured = client._init_kwargs

        self.assertEqual(captured.get("messages_rate_limit"), custom_rate)
        self.assertEqual(captured.get("telemetry_rate_limit"), custom_rate)
        self.assertEqual(captured.get("telemetry_dp_rate_limit"), custom_dp)

    def test_default_rate_limits(self):
        default_rate = "DEFAULT_RATE_LIMIT"
        with patch("tb_gateway_mqtt.RateLimit.__init__", return_value=None), \
                patch("tb_gateway_mqtt.RateLimit.get_rate_limits_by_host",
                      return_value=("DEFAULT_MESSAGES_RATE_LIMIT", "DEFAULT_TELEMETRY_DP_RATE_LIMIT")), \
                patch("tb_gateway_mqtt.RateLimit.get_rate_limit_by_host", return_value="DEFAULT_MESSAGES_RATE_LIMIT"), \
                patch.object(TBDeviceMqttClient, '__init__', new=TestRateLimitInitialization.fake_init):
            client = TBGatewayMqttClient(
                host="localhost",
                port=1883,
                username="dummy_token",
                rate_limit=default_rate,
                dp_rate_limit=default_rate
            )
            captured = client._init_kwargs

        self.assertEqual(captured.get("messages_rate_limit"), "DEFAULT_MESSAGES_RATE_LIMIT")
        self.assertEqual(captured.get("telemetry_rate_limit"), "DEFAULT_TELEMETRY_RATE_LIMIT")
        self.assertEqual(captured.get("telemetry_dp_rate_limit"), "DEFAULT_TELEMETRY_DP_RATE_LIMIT")


if __name__ == '__main__':
    unittest.main()
