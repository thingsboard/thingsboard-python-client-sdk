import psutil
import time
import logging
from tb_device_mqtt import TBClient
# this example illustrates situation, where client send cpu and memory usage every 5 seconds.
# If client receives an update of uploadFrequency attribute, it changes frequency of other attributes publishing.
# Also client is listening to rpc and responds immediately to corresponding rpc methods from server
# ('getCPULoad' or 'getMemoryUsage')

logging.basicConfig(level=logging.DEBUG)
uploadFrequency = 5


# this callback changes global variable defining how often telemetry is sent
def freq_cb(value=None):
    global uploadFrequency
    uploadFrequency = int(value["uploadFrequency"])


# dependently of request method we send different data back
def on_server_side_rpc_request(request_id, request_body):
    print(request_id, request_body)
    if request_body["method"] == "getCPULoad":
        client.respond(request_id, {"CPU percent": psutil.cpu_percent()})
    elif request_body["method"] == "getMemoryUsage":
        client.respond(request_id, {"Memory": psutil.virtual_memory().percent})


client = TBClient("demo.thingsboard.io", "HvbKddqKsxVqowKoSR2J")
client.set_server_side_rpc_request_handler(on_server_side_rpc_request)
client.connect()
# to change upload Frequency we need to subscribe to corresponding attribute
client.subscribe(key="uploadFrequency", callback=freq_cb)
while True:
    client.send_telemetry({"CPU percent": psutil.cpu_percent()})
    client.send_telemetry({"Memory": psutil.virtual_memory().percent})
    time.sleep(uploadFrequency)
