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

# This example demonstrates how to connect to ThingsBoard over SSL using the GatewayClient,
# connect a device, and send telemetry data securely.

import asyncio
import logging
import random

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.gateway.client import GatewayClient

configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)

PLATFORM_HOST = 'localhost'  # Update with your ThingsBoard host
PLATFORM_PORT = 8883  # Default port for MQTT over SSL

# Update with your CA certificate, client certificate, and client key paths. There are no default files generated.
# You can generate them using the following guides:
# Certificates for server - https://thingsboard.io/docs/user-guide/mqtt-over-ssl/
# Certificates for client - https://thingsboard.io/docs/user-guide/certificates/?ubuntuThingsboardX509=X509Leaf
CA_CERT_PATH = "ca_cert.pem"  # Update with your CA certificate path (Default - ca_cert.pem in the examples directory)
CLIENT_CERT_PATH = "cert.pem"  # Update with your client certificate path (Default - cert.pem in the examples directory)
CLIENT_KEY_PATH = "key.pem"  # Update with your client key path (Default - key.pem in the examples directory)


async def main():
    config = GatewayConfig()

    config.host = PLATFORM_HOST
    config.port = PLATFORM_PORT

    config.ca_cert = CA_CERT_PATH
    config.client_cert = CLIENT_CERT_PATH
    config.private_key = CLIENT_KEY_PATH

    client = GatewayClient(config)
    await client.connect()

    device_name = "Test Device B1"
    device_profile = "Test devices"
    logger.info("Connecting device: %s", device_name)
    device_session, publish_results = await client.connect_device(device_name, device_profile, wait_for_publish=True)

    # Sending telemetry data to the connected device
    list_timeseries = [
        TimeseriesEntry(key="temperature", value=random.randint(20, 35)),
        TimeseriesEntry(key="humidity", value=random.randint(40, 80))
    ]
    logger.info("Sending list of timeseries: %s", list_timeseries)
    await client.send_device_timeseries(device_session=device_session, data=list_timeseries, wait_for_publish=True)
    logger.info("List of timeseries sent successfully.")

    await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
