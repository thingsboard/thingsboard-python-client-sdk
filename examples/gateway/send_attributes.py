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

import asyncio

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.service.gateway.client import GatewayClient


configure_logging()
logger = get_logger(__name__)

config = GatewayConfig()
config.host = "localhost"
config.access_token = "YOUR_ACCESS_TOKEN"

device_name = "Test Device B1"
device_profile = "Test devices"


async def main():
    client = GatewayClient(config)

    await client.connect()

    # Connecting device

    logger.info("Connecting device: %s", device_name)
    device_session, publish_results = await client.connect_device(device_name, device_profile, wait_for_publish=True)

    if not device_session:
        logger.error("Failed to register device: %s", device_name)
        return

    logger.info("Device connected successfully: %s", device_name)

    # Send attributes as raw dictionary
    raw_attributes = {
        "maintenance": "scheduled",
        "id": 341,
    }
    logger.info("Sending raw attributes: %s", raw_attributes)
    await client.send_device_attributes(device_session=device_session, data=raw_attributes, wait_for_publish=True)
    logger.info("Raw timeseries sent successfully.")

    # Send single attribute entry
    single_attribute = AttributeEntry(key="location", value="office")
    logger.info("Sending single attribute: %s", single_attribute)
    await client.send_device_attributes(device_session=device_session, data=single_attribute, wait_for_publish=True)
    logger.info("Single attribute sent successfully.")

    # Send multiple attribute entries
    multiple_attributes = [
        AttributeEntry(key="status", value="active"),
        AttributeEntry(key="version", value="1.0.0")
    ]
    logger.info("Sending multiple attributes: %s", multiple_attributes)
    await client.send_device_attributes(device_session=device_session, data=multiple_attributes, wait_for_publish=True)
    logger.info("Multiple attributes sent successfully.")

    # Disconnect device
    logger.info("Disconnecting device: %s", device_name)
    await client.disconnect_device(device_session, wait_for_publish=True)
    logger.info("Device disconnected successfully: %s", device_name)

    # Disconnect client
    await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down.")
