import logging
from tb_device_mqtt import TBClient
import time
logging.basicConfig(level=logging.DEBUG)

telemetry = {"temperature": 41.9, "humidity": 69, "enabled": False, "currentFirmwareVersion": "v1.2.2"}
telemetry_as_array = [{"temperature": 42.0}, {"humidity": 70}, {"enabled": True}, {"currentFirmwareVersion": "v1.2.3"}]
telemetry_with_ts = {"ts": int(round(time.time() * 1000)), "values": {"temperature": 42.1, "humidity": 70}}
telemetry_with_ts_as_array = [{"ts": 1451649600000, "values": {"temperature": 42.2, "humidity": 71}},
                              {"ts": 1451649601000, "values": {"temperature": 42.3, "humidity": 72}}]
attributes = {"sensorModel": "DHT-22", "attribute_2": "value"}

client = TBClient("127.0.0.1", "A2_TEST_TOKEN")
client.connect()
client.send_attributes(attributes)
client.send_telemetry(telemetry)
client.send_telemetry(telemetry_as_array, quality_of_service=1)
client.send_telemetry(telemetry_with_ts)
client.send_telemetry(telemetry_with_ts_as_array)
time.sleep(3)
client.disconnect()
