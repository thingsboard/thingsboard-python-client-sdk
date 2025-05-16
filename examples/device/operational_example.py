import asyncio
import logging
import signal
from datetime import datetime, UTC
from random import randint, uniform

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("tb_mqtt_client").setLevel(logging.DEBUG)


async def attribute_update_callback(update: AttributeUpdate):
    """
    Callback function to handle attribute updates.
    :param update: The attribute update object.
    """
    logger.info("Received attribute update: %s", update.as_dict())


async def rpc_request_callback(request: RPCRequest):
    """
    Callback function to handle RPC requests.
    :param request: The RPC request object.
    :return: A RPC response object.
    """
    logger.info("Received RPC request: %s", request.to_dict())
    response_data = {
        "status": "success",
    }
    response = RPCResponse(request_id=request.request_id,
                           result=response_data,
                           error=None)
    return response


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
        await client.send_attributes(raw_dict)

        logger.info(f"Raw attributes sent: {raw_dict}")

        # 2. Single AttributeEntry
        single_entry = AttributeEntry("mode", "normal")
        await client.send_attributes(single_entry)

        logger.info("Single attribute sent: %s", single_entry)

        # 3. List of AttributeEntry
        attr_entries = [
            AttributeEntry("maxTemperature", 85),
            AttributeEntry("calibrated", True)
        ]
        await client.send_attributes(attr_entries)

        # --- Telemetry ---

        # 1. Raw dict
        raw_dict = {
            "temperature": round(uniform(20.0, 30.0), 2),
            "humidity": 60
        }
        await client.send_telemetry(raw_dict)

        logger.info(f"Raw telemetry sent: {raw_dict}")

        # 2. Single TelemetryEntry (with ts)
        single_entry = TimeseriesEntry("batteryLevel", randint(0, 100))
        await client.send_telemetry(single_entry)

        logger.info("Single telemetry sent: %s", single_entry)

        # 3. List of TelemetryEntry with mixed timestamps
        ts_now = int(datetime.now(UTC).timestamp() * 1000)
        telemetry_entries = []
        for i in range(100):
            telemetry_entries.append(TimeseriesEntry("temperature", i, ts=ts_now-i))
        await client.send_telemetry(telemetry_entries)
        logger.info("List of telemetry sent: %s, it took %r milliseconds", len(telemetry_entries),
                    int(datetime.now(UTC).timestamp() * 1000) - ts_now)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass

    await client.disconnect()
    logger.info("Disconnected cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user.")
