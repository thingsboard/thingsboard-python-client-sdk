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
import time

logging.basicConfig(level=logging.DEBUG)

telemetry_with_ts = {"ts": int(round(time.time() * 1000)), "values": {"temperature": 42.1, "humidity": 70}}


def main():
    client = TBDeviceMqttClient("127.0.0.1", username="A2_TEST_TOKEN")
    client.connect()

    results = []
    result = True

    for i in range(0, 100):
        results.append(client.send_telemetry({"ts": int(round(time.time() * 1000)), "values": {"temperature": 42.1, "humidity": 70}}))

    for tmp_result in results:
        result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS

    print("Result " + str(result))

    client.stop()


if __name__ == '__main__':
    main()
