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

# Example script to send attributes to ThingsBoard using the DeviceClient

import asyncio
import logging

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)


async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    await client.connect()

    # Send attribute as raw dictionary
    raw_attributes = {
        "firmwareVersion": "1.0.3",
        "hardwareModel": "TB-SDK-Device"
    }
    logger.info("Sending raw attributes: %s", raw_attributes)
    await client.send_attributes(raw_attributes)
    logger.info("Raw attributes sent successfully.")

    # Send single attribute entry
    single_attribute = AttributeEntry("mode", "normal")
    logger.info("Sending single attribute: %s", single_attribute)
    await client.send_attributes(single_attribute)
    logger.info("Single attribute sent successfully.")

    # Send list of attributes
    attribute_list = [
        AttributeEntry("location", "Building A"),
        AttributeEntry("status", "active")
    ]
    logger.info("Sending list of attributes: %s", attribute_list)
    await client.send_attributes(attribute_list)
    logger.info("List of attributes sent successfully.")

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
