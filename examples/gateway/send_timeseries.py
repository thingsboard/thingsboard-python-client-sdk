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
import signal
from time import time

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.gateway.client import GatewayClient

configure_logging()
logger = get_logger(__name__)


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

    logger.info("Device connected successfully: %s", device_name)
    stop_event = asyncio.Event()

    def _shutdown_handler():
        stop_event.set()
        asyncio.gather(client.stop(), return_exceptions=True)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)  # noqa
        except NotImplementedError:
            # Windows compatibility fallback
            signal.signal(sig, lambda *_: _shutdown_handler())  # noqa

    loop_counter = 0

    while not stop_event.is_set():
        loop_counter += 1
        logger.info("Sending timeseries data, iteration: %d", loop_counter)

        # Send time series as raw dictionary
        raw_timeseries = {
            "temperature": 25.5,
            "humidity": 60
        }
        logger.info("Sending raw timeseries: %s", raw_timeseries)
        await client.send_device_timeseries(device_session=device_session, data=raw_timeseries, wait_for_publish=True)
        logger.info("Raw timeseries sent successfully.")

        # Send time series as list of dictionaries
        ts = int(time() * 1000)
        list_timeseries = [
            {"ts": ts, "values": {"temperature": 26.0, "humidity": 65}},
            {"ts": ts - 1000, "values": {"temperature": 26.5, "humidity": 70}}
        ]
        logger.info("Sending list of timeseries: %s", list_timeseries)
        await client.send_device_timeseries(device_session=device_session, data=list_timeseries, wait_for_publish=True)
        logger.info("List of timeseries sent successfully.")

        # Send time series as TimeseriesEntry objects
        ts = int(time() * 1000)
        timeseries_entries = [
            TimeseriesEntry(key="temperature%i" % i, value=loop_counter, ts=ts) for i in range(20)
        ]
        logger.info("Sending TimeseriesEntry objects: %s", timeseries_entries)
        await client.send_device_timeseries(device_session=device_session, data=timeseries_entries, wait_for_publish=True)
        logger.info("TimeseriesEntry objects sent successfully.")


        try:
            logger.info("Waiting before next iteration...")
            await asyncio.wait_for(stop_event.wait(), timeout=1)
        except asyncio.TimeoutError:
            logger.info("Going to next iteration...")

    await client.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down.")
