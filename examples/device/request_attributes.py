# Copyright 2023. ThingsBoard
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
logging.basicConfig(level=logging.DEBUG)


def on_attributes_change(result, exception=None):
    if exception is not None:
        print("Exception: " + str(exception))
    else:
        print(result)


def main():
    client = TBDeviceMqttClient("127.0.0.1", 1883, "A2_TEST_TOKEN")
    client.connect()
    client.request_attributes(["atr1", "atr2"], callback=on_attributes_change)
    while not client.stopped:
        time.sleep(1)


if __name__ == '__main__':
    main()
