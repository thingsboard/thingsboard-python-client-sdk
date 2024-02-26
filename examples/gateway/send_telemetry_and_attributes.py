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

import time
import logging
from tb_gateway_mqtt import TBGatewayMqttClient

logging.basicConfig(level=logging.INFO)


attributes = {"atr1": 1, "atr2": True, "atr3": "value3"}
telemetry_simple = {"ts": int(round(time.time() * 1000)), "values": {"key1": "11"}}
telemetry_array = [
    {"ts": int(round(time.time() * 1000)), "values": {"key1": "11"}},
    {"ts": int(round(time.time() * 1000)), "values": {"key2": "22"}}
]


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", username="TEST_GATEWAY_TOKEN")
    # without device connection it is impossible to get any messages
    gateway.connect()

    gateway.gw_send_telemetry("Test Device A2", telemetry_simple)
    gateway.gw_send_telemetry("Test Device A2", telemetry_array)
    gateway.gw_send_attributes("Test Device A2", attributes)
    gateway.disconnect()


if __name__ == '__main__':
    main()
