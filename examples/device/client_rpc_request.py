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
from tb_device_mqtt import TBDeviceMqttClient
logging.basicConfig(level=logging.INFO)


def callback(request_id, resp_body, exception=None):
    if exception is not None:
        logging.error("Exception: " + str(exception))
    else:
        logging.info("request id: {request_id}, response body: {resp_body}".format(request_id=request_id,
                                                                            resp_body=resp_body))


def main():
    client = TBDeviceMqttClient("127.0.0.1", username="A2_TEST_TOKEN")

    client.connect()
    # call "getTime" on server and receive result, then process it with callback
    client.send_rpc_call("getTime", {}, callback)
    try:
        while not client.stopped:
            time.sleep(1)
    except KeyboardInterrupt:
        client.disconnect()
        client.stop()


if __name__ == '__main__':
    main()
