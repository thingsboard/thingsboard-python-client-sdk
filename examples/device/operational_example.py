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
from random import uniform, randint

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse, RPCStatus
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.client import DeviceClient

configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("tb_mqtt_client").setLevel(logging.DEBUG)


async def attribute_update_callback(update: AttributeUpdate):
    """
    Callback function to handle attribute updates.
    :param update: The attribute update object.
    """
    logger.info("Received attribute update: %r", update)


async def rpc_request_callback(request: RPCRequest) -> RPCResponse:
    """
    Callback function to handle RPC requests.
    :param request: The RPC request object.
    :return: An RPCResponse object.
    """
    logger.info("Received RPC request: %r", request)

    if request.method == "getError":
        # Simulate an error response for demonstration purposes
        logger.error("Simulated error for method: %s", request.method)
        try:
            # Simulate some processing that raises an error
            raise RuntimeError("Simulated processing error")
        except RuntimeError as e:
            return RPCResponse.build(request_id=request.request_id, error=e)
    else:
        response_data = {
            "message": f"Response for method {request.method}",
            "params": request.params or {}
        }
        response = RPCResponse.build(request_id=request.request_id, result=response_data)
    return response

async def rpc_response_callback(response: RPCResponse):
    """
    Callback function to handle RPC responses for client side RPC requests.
    :param response: The RPC response object.
    """
    logger.info("Received RPC response in callback: %r", response)


async def attribute_request_callback(requested_attributes_response: RequestedAttributeResponse):
    """
    Callback function to handle requested attributes.
    :param requested_attributes_response: The requested attribute response object.
    """
    logger.info("Received requested attributes response: %r", requested_attributes_response)


async def main():
    stop_event = asyncio.Event()

    def _shutdown_handler():
        stop_event.set()
        asyncio.get_event_loop().run_until_complete(client.stop())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)  # noqa
        except NotImplementedError:
            # Windows compatibility fallback
            signal.signal(sig, lambda *_: _shutdown_handler())  # noqa

    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    client.set_attribute_update_callback(attribute_update_callback)
    client.set_rpc_request_callback(rpc_request_callback)
    await client.connect()

    logger.info("Connected to ThingsBoard.")

    while not stop_event.is_set():
        # --- Attributes ---

        # 1. Raw dict
        raw_dict = {
            "firmwareVersion": "1.0.4",
            "hardwareModel": "TB-SDK-Device"
        }
        logger.info("Sending attributes...")
        raw_publish_result = await client.send_attributes(raw_dict)
        logger.info(f"Raw attributes sent: {raw_dict} with result: {raw_publish_result}")

        # 2. Single AttributeEntry
        single_entry = AttributeEntry("mode", "normal")
        logger.info("Sending single attribute: %s", single_entry)
        single_attribute_publish_result = await client.send_attributes(single_entry)
        logger.info(f"Single attribute sent: {single_entry} with result: {single_attribute_publish_result}")

        # 3. List of AttributeEntry
        attr_entries = [
            AttributeEntry("maxTemperature", 85),
            AttributeEntry("calibrated", True)
        ]
        logger.info("Sending list of attributes: %s", attr_entries)
        attributes_list_publish_result = await client.send_attributes(attr_entries)
        logger.info("List of attributes sent: %s with result: %s", attr_entries, attributes_list_publish_result)

        # --- Telemetry ---

        # 1. Raw dict
        raw_dict = {
            "temperature": round(uniform(20.0, 30.0), 2),
            "humidity": 60
        }
        logger.info("Sending raw telemetry...")
        raw_telemetry_publish_result = await client.send_telemetry(raw_dict)
        logger.info(f"Raw telemetry sent: {raw_dict} with result: {raw_telemetry_publish_result}")

        # 2. Single TimeseriesEntry (with ts)
        single_entry = TimeseriesEntry("batteryLevel", randint(0, 100))
        logger.info("Sending single telemetry: %s", single_entry)
        delivery_future = await client.send_telemetry(single_entry)
        logger.info(f"Single telemetry sent: {single_entry} with delivery future: {delivery_future}")

        # 3. List of TimeseriesEntry with mixed timestamps

        telemetry_entries = []
        for i in range(100):
            telemetry_entries.append(TimeseriesEntry("temperature", i, ts=int(datetime.now(UTC).timestamp() * 1000)-i))
        logger.info("Sending list of telemetry entries with mixed timestamps...")
        telemetry_list_publish_result = await client.send_telemetry(telemetry_entries)
        logger.info("List of telemetry entries sent: %s with result: %s",
                    len(telemetry_entries) if len(telemetry_entries) > 10 else telemetry_entries,
                    telemetry_list_publish_result)

        # --- Attribute Request ---

        logger.info("Requesting attributes...")

        attribute_request = await AttributeRequest.build(["uno"], ["client"])

        logger.info("Sending attribute request: %r", attribute_request)

        await client.send_attribute_request(attribute_request, attribute_request_callback)

        # --- Client-side RPC ---

        logger.info("Sending client side RPC request...")

        rpc_request = await RPCRequest.build("getSomeInformation", {"key1": "value1"})

        logger.info("Sending RPC request: %r", rpc_request)

        try:
            rpc_response = await client.send_rpc_request(rpc_request)
        except TimeoutError as e:
            logger.error("Timeout while sending RPC request: %s", e)
            rpc_response = None

        if rpc_response:
            logger.info("RPC response received: %s", rpc_response)

        rpc_request_2 = await RPCRequest.build("getAnotherInformation", {"param": "value"})

        logger.info("Sending another RPC request: %r", rpc_request_2)

        await client.send_rpc_request(rpc_request_2, rpc_response_callback, wait_for_publish=False)

        try:
            logger.info("Waiting for 1 seconds before next iteration...")
            await asyncio.wait_for(stop_event.wait(), timeout=1)
        except asyncio.TimeoutError:
            logger.info("Going to next iteration...")

    logger.info("Disconnected cleanly.")


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_debug(False)  # Enable debug mode for asyncio
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Interrupted by user.")
