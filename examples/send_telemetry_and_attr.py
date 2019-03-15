import tb_mqtt_client as tb
import json
import logging

logging.basicConfig(level=logging.INFO)
telemetry = json.dumps({"temp": 1001})
attributes = json.dumps({"firmwareVersion": "v2.3.2", "temp": 1})

client = tb.TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
client.connect()
client.send_telemetry(telemetry)
client.send_attributes(attributes)
client.disconnect()
