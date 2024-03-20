# Copyright 2024. ThingsBoard
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#  http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
from time import sleep, time
logging.basicConfig(level=logging.DEBUG)

telemetry = {"temperature": 41.9, "humidity": 69, "enabled": False, "currentFirmwareVersion": "v1.2.2"}
telemetry_as_array = [{"temperature": 42.0}, {"humidity": 70}, {"enabled": True}, {"currentFirmwareVersion": "v1.2.3"}]
telemetry_with_ts = {"ts": int(round(time() * 1000)), "values": {"temperature": 42.1, "humidity": 70}}
telemetry_with_ts_as_array = [{"ts": 1451649600000, "values": {"temperature": 42.2, "humidity": 71}},
                              {"ts": 1451649601000, "values": {"temperature": 42.3, "humidity": 72}}]
attributes = {"sensorModel": "DHT-22", "attribute_2": "value"}

log = logging.getLogger(__name__)


def on_connect(client, userdata, flags, result_code, *extra_params, tb_client):
    if result_code == 0:
        log.info("Connected to ThingsBoard!")
        # Sending data in async way
        tb_client.send_attributes(attributes)
        tb_client.send_telemetry(telemetry)
        tb_client.send_telemetry(telemetry_as_array, quality_of_service=1)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts)
        tb_client.send_telemetry(telemetry_with_ts_as_array)

    # Waiting for data to be delivered
        result = tb_client.send_attributes(attributes)
        result.get()
        log.info("Attribute update sent: " + str(result.rc() == TBPublishInfo.TB_ERR_SUCCESS))
        result = tb_client.send_attributes(attributes)
        result.get()
        log.info("Telemetry update sent: " + str(result.rc() == TBPublishInfo.TB_ERR_SUCCESS))
    else:
        log.error("Failed to connect to ThingsBoard with result code: %d", result_code)
    tb_client.disconnect()


def main():
    client = TBDeviceMqttClient("demo.thingsboard.io", username="KZP2HfvOGieLvdAghCNz")
    client.connect(callback=on_connect)

    while not client.stopped:
        sleep(1)


if __name__ == '__main__':
    main()
