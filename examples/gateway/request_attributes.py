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
import time

from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.INFO)


def callback(result, exception=None):
    if exception is not None:
        logging.error("Exception: " + str(exception))
    else:
        logging.info(result)


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", username="TEST_GATEWAY_TOKEN")
    gateway.connect()
    # Requesting attributes
    gateway.gw_request_shared_attributes("Example Name", ["temperature"], callback)

    try:
        # Waiting for the callback
        while not gateway.stopped:
            time.sleep(1)
    except KeyboardInterrupt:
        gateway.disconnect()
        gateway.stop()


if __name__ == '__main__':
    main()
