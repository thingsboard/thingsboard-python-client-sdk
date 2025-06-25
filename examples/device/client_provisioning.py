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

# Example script to device provisioning using the DeviceClient

import asyncio
from random import randint

from tb_mqtt_client.entities.data.provisioning_request import AccessTokenProvisioningCredentials, ProvisioningRequest
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.client import DeviceClient


async def main():
    provisioning_credentials = AccessTokenProvisioningCredentials(
        provision_device_key='YOUR_PROVISION_DEVICE_KEY',
        provision_device_secret='YOUR_PROVISION_DEVICE_SECRET',
    )
    provisioning_request = ProvisioningRequest('localhost', credentials=provisioning_credentials)
    provisioning_response = await DeviceClient.provision(provisioning_request)

    if provisioning_response.error is not None:
        print(f"Provisioning failed: {provisioning_response.error}")
        return

    print('Provisined device config: ', provisioning_response)

    # Create a DeviceClient instance with the provisioned device config
    client = DeviceClient(provisioning_response.result)
    await client.connect()

    # Send single telemetry entry to provisioned device
    await client.send_telemetry(TimeseriesEntry("batteryLevel", randint(0, 100)))

    await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
