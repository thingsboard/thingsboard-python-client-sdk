# Copyright 2024. ThingsBoard
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#  http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import logging
try:
    import psutil
except ImportError:
    print("Please install psutil using 'pip install psutil' command")
    exit(1)
from tb_device_mqtt import TBDeviceMqttClient
# this example illustrates situation, where client send cpu and memory usage every 5 seconds.
# If client receives an update of uploadFrequency attribute, it changes frequency of other attributes publishing.
# Also client is listening to rpc and responds immediately to corresponding rpc methods from server
# ('getCPULoad' or 'getMemoryUsage')

logging.basicConfig(level=logging.DEBUG)
uploadFrequency = 5


# this callback changes global variable defining how often telemetry is sent
def on_upload_frequency_change(value, error):
    global uploadFrequency
    if "uploadFrequency" in value:
        uploadFrequency = int(value["uploadFrequency"])
    elif "shared" in value and "uploadFrequency" in value["shared"]:
        uploadFrequency = int(value["shared"]["uploadFrequency"])


# dependently of request method we send different data back
def on_server_side_rpc_request(client, request_id, request_body):
    print(client, request_id, request_body)
    if request_body["method"] == "getCPULoad":
        client.send_rpc_reply(request_id, {"CPU percent": psutil.cpu_percent()})
    elif request_body["method"] == "getMemoryUsage":
        client.send_rpc_reply(request_id, {"Memory": psutil.virtual_memory().percent})


client = TBDeviceMqttClient("127.0.0.1", username="A2_TEST_TOKEN")
client.set_server_side_rpc_request_handler(on_server_side_rpc_request)
client.connect()
# to fetch the latest setting for upload frequency configured on the server
client.request_attributes(shared_keys=["uploadFrequency"], callback=on_upload_frequency_change)
# to subscribe to future changes of upload frequency
client.subscribe_to_attribute(key="uploadFrequency", callback=on_upload_frequency_change)


def main():
    while True:
        client.send_telemetry({"cpu": psutil.cpu_percent(), "memory": psutil.virtual_memory().percent})
        print("Sleeping for " + str(uploadFrequency))
        time.sleep(uploadFrequency)


if __name__ == '__main__':
    main()
