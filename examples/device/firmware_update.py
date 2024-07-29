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
from tb_device_mqtt import TBDeviceMqttClient, FW_STATE_ATTR

logging.basicConfig(level=logging.INFO)


def main():
    client = TBDeviceMqttClient("127.0.0.1", username="A2_TEST_TOKEN")
    client.connect()

    client.get_firmware_update()

    # Waiting for firmware to be delivered
    while not client.current_firmware_info[FW_STATE_ATTR] == 'UPDATED':
        time.sleep(1)

    client.disconnect()
    client.stop()


if __name__ == '__main__':
    main()
