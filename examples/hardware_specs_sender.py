import psutil
import time
import logging
from tb_mqtt_client import TbClient
from json import dumps
logging.basicConfig(level=logging.INFO)
uploadFrequency = 5

client = TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")

def freq_cb(input = None):
    global uploadFrequency
    uploadFrequency = int((input["uploadFrequency"]))

client.connect()
client.subscribe(to_rpc=True)
client.subscribe(callback=freq_cb, key="uploadFrequency", quality_of_service=2)
while True:
    client.send_telemetry(dumps({"CPU percent": psutil.cpu_percent()}))
    client.send_telemetry(dumps({"Memory": psutil.virtual_memory().percent}))
    time.sleep(uploadFrequency)


#def on_server_side_rpc_request (requestId, requestBody):
#    client.on_server_side_rpc_response(requestId, {"ololo":"trololo"})
#client.set_server_side_rpc_request_handler(on_server_side_rpc_request)



# curl -X POST -d '{"method": "getTemperature", "params":{}}' http://localhost:8080/api/v1/IAkHBb9N7kKD9ieLRMFN/rpc --header "Content-Type:application/json"