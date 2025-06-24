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

# Example script to handle attribute updates from ThingsBoard using the DeviceClient

import asyncio
from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.service.device.client import DeviceClient

async def attribute_update_callback(update: AttributeUpdate):
    print("Received attribute update:", update)

async def main():
    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    client.set_attribute_update_callback(attribute_update_callback)

    await client.connect()
    print("Waiting for attribute updates... Press Ctrl+C to stop.")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")

    await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
