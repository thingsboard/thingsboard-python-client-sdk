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
from datetime import datetime, UTC
from random import randint, uniform

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.service.gateway.client import GatewayClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("tb_mqtt_client").setLevel(logging.DEBUG)


async def device_attribute_update_callback(update: AttributeUpdate):
    """
    Callback function to handle device attribute updates.
    :param update: The attribute update object.
    """
    logger.info("Received attribute update for device %s: %s", update.device, update.attributes)


async def device_rpc_request_callback(device_name: str, method: str, params: dict):
    """
    Callback function to handle device RPC requests.
    :param device_name: Name of the device
    :param method: RPC method
    :param params: RPC parameters
    :return: RPC response
    """
    logger.info("Received RPC request for device %s: method=%s, params=%s", device_name, method, params)
    
    # Example response based on method
    if method == "getTemperature":
        return {"temperature": round(uniform(20.0, 30.0), 2)}
    elif method == "setLedState":
        state = params.get("state", False)
        return {"success": True, "state": state}
    else:
        return {"error": f"Unsupported method: {method}"}


async def device_disconnect_callback(device_name: str):
    """
    Callback function to handle device disconnections.
    :param device_name: Name of the disconnected device
    """
    logger.info("Device %s disconnected", device_name)


async def main():
    stop_event = asyncio.Event()

    def _shutdown_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)
        except NotImplementedError:
            # Windows compatibility fallback
            signal.signal(sig, lambda *_: _shutdown_handler())

    config = GatewayConfig()
    config.host = "localhost"
    config.access_token = "YOUR_GATEWAY_ACCESS_TOKEN"

    client = GatewayClient(config)
    client.set_device_attribute_update_callback(device_attribute_update_callback)
    client.set_device_rpc_request_callback(device_rpc_request_callback)
    client.set_device_disconnect_callback(device_disconnect_callback)
    await client.connect()

    logger.info("Connected to ThingsBoard as gateway.")

    # Connect devices to the gateway
    device_names = ["sensor-1", "sensor-2", "actuator-1"]
    for device_name in device_names:
        await client.gw_connect_device(device_name)
        logger.info("Connected device: %s", device_name)

    while not stop_event.is_set():
        # Send device attributes
        for device_name in device_names:
            # Send device attributes
            attributes = {
                "firmwareVersion": "1.0.4",
                "serialNumber": f"SN-{randint(1000, 9999)}",
                "deviceType": "sensor" if "sensor" in device_name else "actuator"
            }
            await client.gw_send_attributes(device_name, attributes)
            logger.info("Sent attributes for device %s: %s", device_name, attributes)

            # Send single attribute
            single_attr = AttributeEntry("lastUpdateTime", datetime.now(UTC).isoformat())
            await client.gw_send_attributes(device_name, single_attr)
            logger.info("Sent single attribute for device %s: %s", device_name, single_attr)

            # Send device telemetry
            if "sensor" in device_name:
                # For sensor devices
                telemetry = {
                    "temperature": round(uniform(20.0, 30.0), 2),
                    "humidity": round(uniform(40.0, 80.0), 2),
                    "batteryLevel": randint(1, 100)
                }
                await client.gw_send_telemetry(device_name, telemetry)
                logger.info("Sent telemetry for device %s: %s", device_name, telemetry)

                # Send single telemetry entry
                single_entry = TimeseriesEntry("signalStrength", randint(-90, -30))
                await client.gw_send_telemetry(device_name, single_entry)
                logger.info("Sent single telemetry entry for device %s: %s", device_name, single_entry)
            else:
                # For actuator devices
                telemetry = {
                    "state": "ON" if randint(0, 1) == 1 else "OFF",
                    "powerConsumption": round(uniform(0.1, 5.0), 2),
                    "uptime": randint(1, 1000)
                }
                await client.gw_send_telemetry(device_name, telemetry)
                logger.info("Sent telemetry for device %s: %s", device_name, telemetry)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass

    # Disconnect devices before shutting down
    for device_name in device_names:
        await client.gw_disconnect_device(device_name)
        logger.info("Disconnected device: %s", device_name)

    await client.disconnect()
    logger.info("Disconnected from ThingsBoard.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user.")