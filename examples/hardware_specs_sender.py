import psutil
import time
import logging
from tb_mqtt_client import TbClient
logging.basicConfig(level=logging.INFO)
uploadFrequency = 10

#это под большим вопросом сейчас
def freq_cb(freq):
    print(freq)
    #uploadFrequency = freq

client = TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
client.connect()
client.subscribe(callback=freq_cb, key="uploadFrequency", quality_of_service=2)
while True:
    client.send_telemetry(dict(psutil.cpu_percent()._asdict()))
    client.send_telemetry(dict(psutil.virtual_memory()._asdict()))
    time.sleep(uploadFrequency)
