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
import logging
import signal
from random import uniform, randint
from time import time

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.gateway_attribute_request import GatewayAttributeRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.service.gateway.client import GatewayClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession

# ---- Logging Setup ----
configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("tb_mqtt_client").setLevel(logging.DEBUG)

# ---- Constants ----
GATEWAY_HOST = "localhost"
GATEWAY_PORT = 1883  # Default MQTT port, change if needed
GATEWAY_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"  # Replace with your actual access token

DELAY_BETWEEN_DATA_PUBLISH = 1  # seconds


# ---- Callbacks ----
async def attribute_update_handler(device_session: DeviceSession, update: AttributeUpdate):
    logger.info("Received attribute update for %s: %s",
                device_session.device_info.device_name, update.entries)


async def requested_attributes_handler(device_session: DeviceSession, response: RequestedAttributeResponse):
    logger.info("Requested attributes for %s -> client: %r, shared: %r",
                device_session.device_info.device_name,
                response.client,
                response.shared)


async def device_rpc_request_handler(device_session: DeviceSession,
                                     rpc_request: GatewayRPCRequest) -> GatewayRPCResponse:
    logger.info("Received RPC request for %s: %r", device_session.device_info.device_name, rpc_request)
    response_data = {
        "status": "success",
        "echo_method": rpc_request.method,
        "params": rpc_request.params
    }
    return GatewayRPCResponse.build(device_session.device_info.device_name, rpc_request.request_id, response_data)


# ---- Main ----
async def main():
    stop_event = asyncio.Event()

    def _shutdown_handler():
        stop_event.set()
        asyncio.create_task(client.stop())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _shutdown_handler())

    # ---- Gateway Config ----
    config = GatewayConfig()
    config.host = GATEWAY_HOST
    config.port = GATEWAY_PORT
    config.access_token = GATEWAY_ACCESS_TOKEN

    global client
    client = GatewayClient(config)

    await client.connect()
    logger.info("Gateway connected to ThingsBoard.")

    # ---- Register Devices ----
    devices = [
        ("Test Device A1", "Test devices"),
        ("Test Device B1", "Test devices")
    ]
    sessions = {}

    for name, profile in devices:
        session, _ = await client.connect_device(name, profile, wait_for_publish=True)
        if not session:
            logger.error("Failed to connect %s", name)
            continue
        sessions[name] = session
        logger.info("Device connected: %s", name)

        # Register callbacks for each device
        client.device_manager.set_attribute_update_callback(session.device_info.device_id, attribute_update_handler)
        client.device_manager.set_attribute_response_callback(session.device_info.device_id,
                                                              requested_attributes_handler)
        client.device_manager.set_rpc_request_callback(session.device_info.device_id, device_rpc_request_handler)

    # ---- Main loop ----
    while not stop_event.is_set():
        iteration_start = time()

        for device_name, session in sessions.items():
            logger.info("Publishing data for %s", device_name)

            # --- Attributes ---
            raw_attrs = {"firmwareVersion": "2.0.0", "location": "office"}
            await client.send_device_attributes(session, raw_attrs, wait_for_publish=True)

            single_attr = AttributeEntry("mode", "auto")
            await client.send_device_attributes(session, single_attr, wait_for_publish=True)

            multi_attrs = [
                AttributeEntry("maxTemp", randint(60, 90)),
                AttributeEntry("calibrated", True)
            ]
            await client.send_device_attributes(session, multi_attrs, wait_for_publish=True)

            # --- Telemetry ---
            raw_ts = {"temperature": round(uniform(20, 30), 2), "humidity": randint(40, 80)}
            await client.send_device_timeseries(session, raw_ts, wait_for_publish=True)

            list_ts = [
                {"ts": int(time() * 1000), "values": {"temperature": 25.5}},
                {"ts": int(time() * 1000) - 1000, "values": {"humidity": 65}}
            ]
            await client.send_device_timeseries(session, list_ts, wait_for_publish=True)

            ts_entries = [TimeseriesEntry(f"temp_{i}", randint(0, 50)) for i in range(5)]
            await client.send_device_timeseries(session, ts_entries, wait_for_publish=True)

            # --- Attribute Request ---
            attr_req = await GatewayAttributeRequest.build(
                device_session=session,
                client_keys=["firmwareVersion", "mode"]
            )
            await client.send_device_attributes_request(session, attr_req, wait_for_publish=True)

        # ---- Delay handling ----
        try:
            timeout = DELAY_BETWEEN_DATA_PUBLISH - (time() - iteration_start)
            if timeout > 0:
                await asyncio.wait_for(stop_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    # ---- Disconnect devices ----
    for session in sessions.values():
        await client.disconnect_device(session, wait_for_publish=True)
    await client.disconnect()
    logger.info("Gateway disconnected cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
