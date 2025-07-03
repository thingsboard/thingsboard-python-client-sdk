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

# Example script to send a client-side RPC request to ThingsBoard using the DeviceClient

import asyncio
import logging

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)


async def rpc_response_callback(response: RPCResponse):
    logger.info("Received RPC response:", response)


async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    await client.connect()

    # Send client-side RPC and wait for response
    rpc_request = await RPCRequest.build("getTime", {})
    try:
        response = await client.send_rpc_request(rpc_request)
        logger.info("Received response:", response)
    except TimeoutError:
        logger.info("RPC request timed out")

    # Send client-side RPC with callback
    rpc_request_2 = await RPCRequest.build("getStatus", {})
    await client.send_rpc_request(rpc_request_2, rpc_response_callback, wait_for_publish=False)

    await asyncio.sleep(5)
    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
