#  Copyright 2025 ThingsBoard
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Example script to update firmware using the DeviceClient

import asyncio
import logging
from time import monotonic

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.service.device.client import DeviceClient

configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)

firmware_received = asyncio.Event()
firmware_update_timeout = 30


async def firmware_update_callback(_, payload):
    logger.info(f"Firmware update payload received: {payload}")
    firmware_received.set()


async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    await client.connect()

    await client.update_firmware(on_received_callback=firmware_update_callback)

    update_started = monotonic()
    while not firmware_received.is_set() and monotonic() - update_started < firmware_update_timeout:
        await asyncio.sleep(1)

    await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
