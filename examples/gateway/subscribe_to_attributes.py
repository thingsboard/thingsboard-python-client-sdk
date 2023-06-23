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

import logging.handlers
import time

from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    logging.debug("Callback for attributes, %r", result)


def callback_for_everything(result):
    logging.debug("Everything goes here, %r", result)


def callback_for_specific_attr(result):
    logging.debug("Specific attribute callback, %r", result)


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", 1883, "TEST_GATEWAY_TOKEN")
    gateway.connect()
    # without device connection it is impossible to get any messages
    gateway.gw_connect_device("ImageTest")

    gateway.gw_subscribe_to_all_attributes(callback_for_everything)

    gateway.gw_subscribe_to_attribute("ImageTest", "image", callback_for_specific_attr)

    sub_id = gateway.gw_subscribe_to_all_device_attributes("ImageTest", callback)
    gateway.gw_unsubscribe(sub_id)

    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
