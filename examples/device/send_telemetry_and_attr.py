import logging
from tb_device_mqtt import TBClient

logging.basicConfig(level=logging.DEBUG)
telemetry = {"temperature": 34541}
telemetry_2 = [{"temp": 1001}, {"kek": "lol"}]
telemetry_3 = {"ts":1451649600512, "values":{"key1":"222", "key2":"123"}}
attributes = {"firmwareVersion": "v2.3.2", "temp": 1}

client = TBClient("127.0.0.1", "A2_TEST_TOKEN")
client.connect()
client.send_attributes(attributes)
client.send_telemetry(telemetry)
client.send_telemetry(telemetry_3)
client.send_telemetry(telemetry_2, quality_of_service=True, blocking=True)
client.disconnect()
