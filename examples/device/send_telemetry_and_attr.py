# Copyright 2023. ThingsBoard
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
import time
logging.basicConfig(level=logging.DEBUG)

telemetry = {"temperature": 41.9, "humidity": 69, "enabled": False, "currentFirmwareVersion": "v1.2.2"}
telemetry_as_array = [{"temperature": 42.0}, {"humidity": 70}, {"enabled": True}, {"currentFirmwareVersion": "v1.2.3"}]
telemetry_with_ts = {"ts": int(round(time.time() * 1000)), "values": {"temperature": 42.1, "humidity": 70}}
telemetry_with_ts_as_array = [{"ts": 1451649600000, "values": {"temperature": 42.2, "humidity": 71}},
                              {"ts": 1451649601000, "values": {"temperature": 42.3, "humidity": 72}}]
attributes = {"sensorModel": "DHT-22", "attribute_2": "value"}


def main():
    client = TBDeviceMqttClient("127.0.0.1", 1883, "A2_TEST_TOKEN")
    client.connect()
    # Sending data in async way
    client.send_attributes(attributes)
    client.send_telemetry(telemetry)
    client.send_telemetry(telemetry_as_array, quality_of_service=1)
    client.send_telemetry(telemetry_with_ts)
    client.send_telemetry(telemetry_with_ts_as_array)

    # Waiting for data to be delivered
    result = client.send_attributes(attributes)
    result.get()
    print("Attribute update sent: " + str(result.rc() == TBPublishInfo.TB_ERR_SUCCESS))
    result = client.send_attributes(attributes)
    result.get()
    print("Telemetry update sent: " + str(result.rc() == TBPublishInfo.TB_ERR_SUCCESS))
    client.disconnect()


if __name__ == '__main__':
    main()
