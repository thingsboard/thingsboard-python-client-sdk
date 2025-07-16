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
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.service.gateway.client import GatewayClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession

configure_logging()
logger = get_logger(__name__)


async def device_rpc_request_handler(device_session: DeviceSession, rpc_request: GatewayRPCRequest) -> GatewayRPCResponse:
    """
    Callback to handle RPC requests from the device.
    :param device_session: Device session for which the request was made.
    :param rpc_request: RPC request from the platform.
    """
    logger.info("Received RPC request for device %s: %s", device_session.device_info.device_name, rpc_request)

    response_data = {
        "status": "success",
        "message": f"RPC request '{rpc_request.method}' processed successfully.",
        "data": {
            "device_name": device_session.device_info.device_name,
            "request_id": rpc_request.request_id,
            "method": rpc_request.method,
            "params": rpc_request.params
        }
    }

    rpc_response = GatewayRPCResponse.build(device_session.device_info.device_name, rpc_request.request_id, response_data)

    logger.info("Sending RPC response for request id %r: %r", rpc_request.request_id, rpc_response)

    return rpc_response


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

    # Register callback for requested attributes
    client.device_manager.set_rpc_request_callback(device_session.device_info.device_id, device_rpc_request_handler)

    if not device_session:
        logger.error("Failed to register device: %s", device_name)
        return
    logger.info("Device connected successfully: %s", device_name)

    # Loop to keep the client running and processing RPC requests
    try:
        logger.info("Client loop started, waiting for RPC requests...")
        while True:
            await asyncio.sleep(1)  # Keep the loop running
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Client loop stopped, shutting down.")

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
