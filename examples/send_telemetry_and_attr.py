import tb_device_mqtt as tb
import logging

logging.basicConfig(level=logging.DEBUG)
#telemetry = {"temp": 1001}
#telemetry = [{"temp": 1001}, {"kek":"lol"}]
telemetry = {"ts":1451649600512, "values":{"key1":"666", "key2":"value2"}}
attributes = {"firmwareVersion": "v2.3.2", "temp": 1}

client = tb.TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
client.connect()
client.send_telemetry(telemetry)
client.send_attributes(attributes)
client.disconnect()
