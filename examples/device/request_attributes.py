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
import time

from tb_device_mqtt import TBDeviceMqttClient
logging.basicConfig(level=logging.INFO)


def on_attributes_change(result, exception=None):
    # This is a callback function that will be called when we receive the response from the server
    if exception is not None:
        logging.error("Exception: " + str(exception))
    else:
        logging.info(result)


def main():
    client = TBDeviceMqttClient("127.0.0.1", username="A2_TEST_TOKEN")
    client.connect()
    # Sending data to retrieve it later
    client.send_attributes({"atr1": "value1", "atr2": "value2"})
    # Requesting attributes
    client.request_attributes(["atr1", "atr2"], callback=on_attributes_change)
    try:
        # Waiting for the callback
        while not client.stopped:
            time.sleep(1)
    except KeyboardInterrupt:
        client.disconnect()
        client.stop()


if __name__ == '__main__':
    main()
