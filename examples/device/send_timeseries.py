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
from random import uniform, randint
from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.client import DeviceClient

async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    await client.connect()

    # Send time series as raw dictionary
    await client.send_timeseries({
        "temperature": round(uniform(20.0, 30.0), 2),
        "humidity": randint(30, 70)
    })

    # Send single time series entry
    await client.send_timeseries(TimeseriesEntry("batteryLevel", randint(0, 100)))

    # Send a list of time series entries
    entries = [
        TimeseriesEntry("vibration", 0.05),
        TimeseriesEntry("speed", 123)
    ]
    await client.send_timeseries(entries)

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
