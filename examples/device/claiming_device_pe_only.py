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

import logging
from time import sleep

from tb_device_mqtt import TBDeviceMqttClient

logging.basicConfig(level=logging.DEBUG)

THINGSBOARD_HOST = "127.0.0.1"
DEVICE_ACCESS_TOKEN = "DEVICE_ACCESS_TOKEN"

SECRET_KEY = "DEVICE_SECRET_KEY"  # Customer should write this key in device claiming widget
DURATION = 30000  # In milliseconds (30 seconds)


def main():
    client = None
    try:
        client = TBDeviceMqttClient(THINGSBOARD_HOST, username=DEVICE_ACCESS_TOKEN)
        client.connect()

        info = client.claim(secret_key=SECRET_KEY, duration=DURATION)
        if info.rc() == 0:
            print("Claiming request was sent successfully.")
        else:
            print(f"Failed to send claiming request. Result code: {info.rc()}")

        sleep(DURATION / 1000)

    except Exception as e:
        logging.exception("Failed to execute device claiming: %s", e)

    finally:
        if client is not None:
            client.stop()
        print("Connection closed.")


if __name__ == '__main__':
    main()
