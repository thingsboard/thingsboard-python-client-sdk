import psutil
import time
import logging
from tb_device_mqtt import TBClient
logging.basicConfig(level=logging.DEBUG)
uploadFrequency = 5
client = TBClient("127.0.0.1", "A2_TEST_TOKEN")
#client = TBClient("demo.thingsboard.io", "HvbKddqKsxVqowKoSR2J")

def freq_cb(value=None):
    global uploadFrequency
    uploadFrequency = int(value["uploadFrequency"])


def on_server_side_rpc_request(request_id, request_body):
    print(1)
    if request_body["method"] == "getCPULoad":
        client.respond(request_id, {"CPU percent": psutil.cpu_percent()})
    if request_body["method"] == "getMemoryUsage":
        client.respond(request_id, {"Memory": psutil.virtual_memory().percent})


client.set_server_side_rpc_request_handler(on_server_side_rpc_request)
client.connect()
client.subscribe(key="uploadFrequency", callback=freq_cb)
while True:
    client.send_telemetry({"CPU percent": psutil.cpu_percent()})
    client.send_telemetry({"Memory": psutil.virtual_memory().percent})
    time.sleep(uploadFrequency)
