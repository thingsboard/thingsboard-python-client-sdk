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

# Example script to claim a device using ThingsBoard DeviceClient

import asyncio
import logging

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)


# Constants for connection
PLATFORM_HOST = "localhost"  # Replace with your ThingsBoard host
DEVICE_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"  # Replace with your device's access token

# Constants for claiming
CLAIMING_DURATION = 120  # Default claiming duration in seconds
CLAIMING_SECRET_KEY = "YOUR_SECRET_KEY"  # Replace with your actual secret key


async def main():
    # Create device config
    config = DeviceConfig()
    config.host = PLATFORM_HOST
    config.access_token = DEVICE_ACCESS_TOKEN

    # Create device client
    client = DeviceClient(config)
    await client.connect()

    # Build claim request with secret key and optional duration (in seconds)
    claim_request = ClaimRequest.build(secret_key=CLAIMING_SECRET_KEY, duration=CLAIMING_DURATION)

    # Send claim request
    result: PublishResult = await client.claim_device(claim_request, wait_for_publish=True, timeout=CLAIMING_DURATION + 10)
    if result.is_successful():
        logger.info(f"Claiming request was sent successfully. Please use the secret key '{CLAIMING_SECRET_KEY}' to claim the device from the dashboard.")
    else:
        logger.error(f"Failed to send claiming request. Result: {result}")

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
