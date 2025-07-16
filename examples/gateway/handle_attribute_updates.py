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
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.service.gateway.client import GatewayClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession

configure_logging()
logger = get_logger(__name__)


async def attribute_update_handler(device_session: DeviceSession, attribute_update: AttributeUpdate):
    """
    Callback to handle attribute updates.
    :param device_session: Device session for which attributes were requested.
    :param attribute_update: Updated attributes .
    """
    logger.info("Received attribute update for device %s: %s",
                device_session.device_info.device_name, attribute_update.entries)


async def main():
    config = GatewayConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"
    client = GatewayClient(config)

    await client.connect()

    # Connecting device

    device_name = "Test Device A1"
    device_profile = "Test devices"
    logger.info("Connecting device: %s", device_name)
    device_session, publish_results = await client.connect_device(device_name, device_profile, wait_for_publish=True)

    if not device_session:
        logger.error("Failed to register device: %s", device_name)
        return

    # Register callback for requested attributes
    client.device_manager.set_attribute_update_callback(device_session.device_info.device_id, attribute_update_handler)

    logger.info("Device connected successfully: %s", device_name)

    # Loop to keep the client running and processing Attribute updates
    try:
        logger.info("Client loop started, waiting for Attribute updates...")
        while True:
            await asyncio.sleep(1)  # Keep the loop running
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Client loop stopped, shutting down.")

    # Disconnect device
    logger.info("Disconnecting device: %s", device_name)
    await client.disconnect_device(device_session, wait_for_publish=True)
    logger.info("Device disconnected successfully: %s", device_name)

    await asyncio.sleep(.1)  # Wait for disconnection to complete

    # Disconnect client
    await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down.")
