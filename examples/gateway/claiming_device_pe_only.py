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

import logging

from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)

THINGSBOARD_HOST = "127.0.0.1"
GATEWAY_ACCESS_TOKEN = "GATEWAY_ACCESS_TOKEN"

DEVICE_NAME = "DEVICE_NAME"
SECRET_KEY = "DEVICE_SECRET_KEY"  # Customer should write this key in device claiming widget
DURATION = 30000  # In milliseconds (30 seconds)


def main():
    client = TBGatewayMqttClient(THINGSBOARD_HOST, username=GATEWAY_ACCESS_TOKEN)
    client.connect()

    """
    You are able to provide every parameter or pass claiming request like:
    request_example = {
                       "DEVICE A": {
                           "secretKey": "DEVICE_A_SECRET_KEY",
                           "durationMs": "30000"
                           },
                       "DEVICE B": {
                           "secretKey": "DEVICE_B_SECRET_KEY",
                           "durationMs": "60000"
                       }
    
    info = client.gw_claim(claiming_request=request_example).wait_for_publish()
    
    """

    client.gw_connect_device(DEVICE_NAME)

    info = client.gw_claim(device_name=DEVICE_NAME, secret_key=SECRET_KEY, duration=DURATION)
    info.wait_for_publish()

    if info.rc == 0:
        print("Claiming request was sent.")
    client.stop()


if __name__ == '__main__':
    main()
