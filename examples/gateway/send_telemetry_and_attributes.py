import logging
from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)
import time

attributes = {"atr1": 1, "atr2": True, "atr3": "value3"}
telemetry_simple = {"ts": 1, "values": {"key1": "11"}}
telemetry_array = [
    {"ts": 1, "values": {"key1": "11"}},
    {"ts": 2, "values": {"key2": "22"}}
]

gateway = TBGatewayMqttClient("127.0.0.1", "TEST_GATEWAY_TOKEN")
# without device connection it is impossible to get any messages
gateway.gw_connect_device("Test Device A2")
gateway.connect()

gateway.gw_send_telemetry("Test Device A2", telemetry_simple)
gateway.gw_send_telemetry("Test Device A2", telemetry_array)
gateway.gw_send_attributes("Test Device A2", attributes)
while True:
    time.sleep(1)
