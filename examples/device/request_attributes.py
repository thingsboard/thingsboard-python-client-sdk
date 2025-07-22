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

# Example script to request attributes from ThingsBoard using the DeviceClient

import asyncio
import logging

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)

response_received = asyncio.Event()

async def attribute_request_callback(response: RequestedAttributeResponse):
    logger.info("Received attribute response: %r", response)
    response_received.set()

async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    await client.connect()

    # Send client attribute to have it available for request
    await client.send_attributes({"currentTemperature": 22.5})

    await asyncio.sleep(.1)

    # Request specific attributes
    request = await AttributeRequest.build(["targetTemperature"], ["currentTemperature"])
    await client.send_attribute_request(request, attribute_request_callback)

    logger.info("Attribute request sent. Waiting for response...")
    try:
        await asyncio.wait_for(response_received.wait(), timeout=10)
    except (asyncio.CancelledError, TimeoutError):
        logger.info("Attribute request cancelled.")

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
