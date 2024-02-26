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
logging.basicConfig(level=logging.DEBUG)


def main():
    """
    We can provide the following parameters to provisioning function:
      host - required - Host of ThingsBoard
      provision_device_key - required - device provision key from device profile
      provision_device_secret - required - device provision secret from device profile
      port=1883 - not required - MQTT port of ThingsBoard instance
      device_name=None - may be generated on ThingsBoard - You may pass here name for device, if this parameter is not assigned, the name will be generated

      ### Credentials type = ACCESS_TOKEN

      access_token=None - may be generated on ThingsBoard - You may pass here some access token and it will be saved as accessToken for device on ThingsBoard.

      ### Credentials type = MQTT_BASIC

      client_id=None - not required (if username is not None) - You may pass here client Id for your device and use it later for connecting
      username=None - not required (if client id is not None) - You may pass here username for your client and use it later for connecting
      password=None - not required - You may pass here password and use it later for connecting

      ### Credentials type = X509_CERTIFICATE
      hash=None - required (If you want to use this credentials type) - You should pass here public key of the device, generated from mqttserver.jks

    """

    # Call device provisioning, to do this we don't need an instance of the TBDeviceMqttClient to provision device

    THINGSBOARD_HOST = "mqtt.thingsboard.cloud"

    credentials = TBDeviceMqttClient.provision(THINGSBOARD_HOST, "PROVISION_DEVICE_KEY", "PROVISION_DEVICE_SECRET")

    if credentials is not None and credentials.get("status") == "SUCCESS":
        username = None
        password = None
        client_id = None
        if credentials["credentialsType"] == "ACCESS_TOKEN":
            username = credentials["credentialsValue"]
        elif credentials["credentialsType"] == "MQTT_BASIC":
            username = credentials["credentialsValue"]["userName"]
            password = credentials["credentialsValue"]["password"]
            client_id = credentials["credentialsValue"]["clientId"]

        client = TBDeviceMqttClient(THINGSBOARD_HOST, username=username, password=password, client_id=client_id)
        client.connect()
        # Other code

        client.stop()


if __name__ == '__main__':
    main()
