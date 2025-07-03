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

# This example demonstrates how to send time series data from a device to ThingsBoard using the DeviceClient.

import asyncio
import logging
from random import uniform, randint
from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)


async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    await client.connect()

    # Send time series as raw dictionary
    raw_timeseries = {
        "temperature": round(uniform(20.0, 30.0), 2),
        "humidity": randint(30, 70)
    }
    logger.info("Sending raw timeseries: %s", raw_timeseries)
    await client.send_timeseries(raw_timeseries)
    logger.info("Raw timeseries sent successfully.")

    # Send single time series entry
    single_entry = TimeseriesEntry("pressure", round(uniform(950.0, 1050.0), 2))
    logger.info("Sending single timeseries entry: %s", single_entry)
    await client.send_timeseries(single_entry)
    logger.info("Single timeseries entry sent successfully.")

    # Send a list of time series entries
    entries = [
        TimeseriesEntry("vibration", 0.05),
        TimeseriesEntry("speed", 123)
    ]
    logger.info("Sending list of timeseries entries: %s", entries)
    await client.send_timeseries(entries)
    logger.info("List of timeseries entries sent successfully.")

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
