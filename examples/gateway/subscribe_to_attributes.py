#      Copyright 2020. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#

import logging.handlers
import time

from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)


def callback(gateway, result):
    logging.debug("Gateway %r. Callback for attributes, %r", gateway, result)


def callback_for_everything(gateway, result):
    logging.debug("Gateway %r. Everything goes here, %r", gateway, result)


def callback_for_specific_attr(gateway, result):
    logging.debug("Gateway %r. Specific attribute callback, %r", gateway, result)


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", "TEST_GATEWAY_TOKEN")
    gateway.connect()
    # without device connection it is impossible to get any messages
    gateway.gw_connect_device("Test Device A2")

    gateway.gw_subscribe_to_all_attributes(callback_for_everything)

    gateway.gw_subscribe_to_attribute("Test Device A2", "temperature", callback_for_specific_attr)

    sub_id = gateway.gw_subscribe_to_all_device_attributes("Test Device A2", callback)
    gateway.gw_unsubscribe(sub_id)

    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
