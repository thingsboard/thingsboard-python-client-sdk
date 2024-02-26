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
from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.INFO)


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", username="TEST_GATEWAY_TOKEN")
    gateway.connect()
    gateway.gw_connect_device("Example Name")
    # device disconnecting will not delete device, gateway just stops receiving messages
    gateway.gw_disconnect_device("Example Name")
    gateway.disconnect()


if __name__ == '__main__':
    main()
