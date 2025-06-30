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

# This example demonstrates how to connect to ThingsBoard over SSL using the DeviceClient and send telemetry.

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


    # Send telemetry entry
    await client.send_timeseries(TimeseriesEntry("batteryLevel", randint(0, 100)))

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
