import psutil
import time
import logging
from tb_mqtt_client import TbClient
from json import dumps
logging.basicConfig(level=logging.INFO)
uploadFrequency = 5

client = TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
def freq_cb(value=None):
    global uploadFrequency
    uploadFrequency = int(value["uploadFrequency"])


def on_server_side_rpc_request(request_id, request_body):
    if request_body["method"] == "getCPULoad":
        client.respond(request_id, {"CPU percent": psutil.cpu_percent()})
    if request_body["method"] == "getMemoryUsage":
        client.respond(request_id, {"Memory": psutil.virtual_memory().percent})

client.set_server_side_rpc_request_handler(on_server_side_rpc_request)
client.connect()
client.subscribe(to_rpc=True)
client.subscribe(callback=freq_cb, key="uploadFrequency", quality_of_service=2)
while True:
    client.send_telemetry(dumps({"CPU percent": psutil.cpu_percent()}))
    client.send_telemetry(dumps({"Memory": psutil.virtual_memory().percent}))
    time.sleep(uploadFrequency)
