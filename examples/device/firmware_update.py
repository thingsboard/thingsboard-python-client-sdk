# Copyright 2025. ThingsBoard
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
from random import randint
from tb_device_mqtt import TBDeviceMqttClient, TBFirmwareState, FW_STATE_ATTR, FW_TITLE_ATTR

logging.basicConfig(level=logging.INFO)

def on_firmware_received_example(client, firmware_data, version_to):
    client.update_firmware_info(state = TBFirmwareState.UPDATING)
    time.sleep(1)

    with open(client.firmware_info.get(FW_TITLE_ATTR), "wb") as firmware_file:
        firmware_file.write(firmware_data)

    random_value = randint(0, 5)
    if random_value > 3:
        logging.error('Dummy fail! Do not panic, just restart and try again the chance of this fail is ~20%')
        client.update_firmware_info(state = TBFirmwareState.FAILED, error = "Dummy fail! Do not panic, just restart and try again the chance of this fail is ~20%")
    else:
        logging.info("Successfully updated!")
        client.update_firmware_info(version = version_to, state = TBFirmwareState.UPDATED)


def main():
    client = TBDeviceMqttClient("127.0.0.1", username="A2_TEST_TOKEN")
    client.connect()

    client.get_firmware_update()

    # Waiting for firmware to be delivered
    while not client.current_firmware_info[FW_STATE_ATTR] == TBFirmwareState.UPDATED.value:
        time.sleep(1)

    client.disconnect()
    client.stop()


if __name__ == '__main__':
    main()
