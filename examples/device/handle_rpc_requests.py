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

# Example script to handle RPC requests from ThingsBoard using the DeviceClient

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


async def rpc_request_callback(request: RPCRequest) -> RPCResponse:
    logger.info("Received RPC: %r", request)

    if request.method == "ping":
        return RPCResponse.build(request_id=request.request_id, result={"pong": True})
    else:
        return RPCResponse.build(request_id=request.request_id, result={"message": "Unknown method"})

async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    client.set_rpc_request_callback(rpc_request_callback)

    await client.connect()
    logger.info("Waiting for RPCs... Press Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down...")

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
