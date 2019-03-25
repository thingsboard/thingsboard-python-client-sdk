import logging
from tb_device_mqtt import TBClient

logging.basicConfig(level=logging.DEBUG)
telemetry = {"temperature": 100}
#telemetry = [{"temp": 1001}, {"kek":"lol"}]
#telemetry = {"ts":1451649600512, "values":{"key1":"222", "key2":"123"}}
#attributes = {"firmwareVersion": "v2.3.2", "temp": 1}

client = TBClient("demo.thingsboard.io", "642e059cd9a5749773f8b49864e834cbe8a2335c5e8c1e71b2efc761bad54543")
client.connect()
client.send_telemetry(telemetry)
#client.send_attributes(attributes)
client.disconnect()
