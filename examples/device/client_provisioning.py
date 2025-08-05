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
import logging
from random import randint

from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.entities.data.provisioning_request import AccessTokenProvisioningCredentials, ProvisioningRequest
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.client import DeviceClient

configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)


async def main():
    provisioning_credentials = AccessTokenProvisioningCredentials(
        provision_device_key='YOUR_PROVISION_DEVICE_KEY',
        provision_device_secret='YOUR_PROVISION_DEVICE_SECRET',
    )
    provisioning_request = ProvisioningRequest('localhost', credentials=provisioning_credentials)
    provisioning_response = await DeviceClient.provision(provisioning_request)

    if provisioning_response.error is not None:
        logger.error(f"Provisioning failed: {provisioning_response.error}")
        return

    logger.info('Provisioned device configuration: ', provisioning_response)

    # Create a DeviceClient instance with the provisioned device configuration
    client = DeviceClient(provisioning_response.result)
    await client.connect()

    # Send single telemetry entry to the provisioned device
    await client.send_timeseries(TimeseriesEntry("batteryLevel", randint(0, 100)))

    await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
