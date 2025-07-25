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
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.service.gateway.client import GatewayClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession

configure_logging()
logger = get_logger(__name__)


async def requested_attributes_handler(device_session: DeviceSession, response: RequestedAttributeResponse):
    """
    Callback to handle requested attributes.
    :param device_session: Device session for which attributes were requested.
    :param response: Response containing requested attributes.
    """
    logger.info("Received attributes for device %s, client attributes: %r, shared attributes: %r",
                device_session.device_info.device_name,
                response.client,
                response.shared)


async def main():
    config = GatewayConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"
    client = GatewayClient(config)

    await client.connect()

    # Connecting device

    device_name = "Test Device B1"
    device_profile = "Test devices"
    logger.info("Connecting device: %s", device_name)
    device_session, publish_results = await client.connect_device(device_name, device_profile, wait_for_publish=True)

    if not device_session:
        logger.error("Failed to register device: %s", device_name)
        return

    # Register callback for requested attributes
    client.device_manager.set_attribute_response_callback(device_session.device_info.device_id, requested_attributes_handler)

    logger.info("Device connected successfully: %s", device_name)

    # Send attributes to request them later
    attributes = [
        AttributeEntry(key="maintenance", value="scheduled"),
        AttributeEntry(key="id", value=341),
        AttributeEntry(key="location", value="office")
    ]
    logger.info("Sending attributes: %s", attributes)
    await client.send_device_attributes(device_session=device_session, data=attributes, wait_for_publish=True)
    logger.info("Attributes sent successfully.")

    await asyncio.sleep(1)  # Wait for attributes to be processed

    # Request attributes for the device
    logger.info("Requesting attributes for device: %s", device_name)
    attributes_to_request = ["maintenance", "id", "location"]
    attribute_request = await GatewayAttributeRequest.build(device_session=device_session, client_keys=attributes_to_request)

    await client.send_device_attributes_request(device_session, attribute_request, wait_for_publish=True)

    # Trying to request shared attribute "state" (Response will be empty if not set)

    logger.info("Requesting shared attribute 'state' for device: %s", device_name)
    shared_attribute_request = await GatewayAttributeRequest.build(device_session=device_session, shared_keys=["state"])
    await client.send_device_attributes_request(device_session, shared_attribute_request, wait_for_publish=True)

    await asyncio.sleep(1)  # Wait for the response to be processed

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
