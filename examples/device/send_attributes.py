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

# Example script to send attributes to ThingsBoard using the DeviceClient

import asyncio
from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.service.device.client import DeviceClient

async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    await client.connect()

    # Send attribute as raw dictionary
    await client.send_attributes({
        "firmwareVersion": "1.0.4",
        "hardwareModel": "TB-SDK-Device"
    })

    # Send single attribute entry
    await client.send_attributes(AttributeEntry("mode", "normal"))

    # Send list of attributes
    await client.send_attributes([
        AttributeEntry("maxTemperature", 85),
        AttributeEntry("calibrated", True)
    ])

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
