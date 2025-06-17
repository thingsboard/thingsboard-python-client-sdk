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

from asyncio import Event

from gmqtt import Client as GMQTTClient
from orjson import loads

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.constants.mqtt_topics import PROVISION_RESPONSE_TOPIC
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest
from tb_mqtt_client.entities.data.provisioning_response import ProvisioningResponse
from tb_mqtt_client.service.message_dispatcher import JsonMessageDispatcher

logger = get_logger(__name__)


class ProvisioningClient:
    def __init__(self, host, port, provision_request: 'ProvisioningRequest'):
        self._log = logger
        self._stop_event = Event()
        self._host = host
        self._port = port
        self._provision_request = provision_request
        self._client_id = "provision"
        self._client = GMQTTClient(self._client_id)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._provisioned = Event()
        self._device_config: 'DeviceConfig' = None
        self._json_message_dispatcher = JsonMessageDispatcher()

    def _on_connect(self, client, _, rc, __):
        if rc == 0:
            self._log.debug("[Provisioning client] Connected to ThingsBoard")
            client.subscribe(PROVISION_RESPONSE_TOPIC)
            topic, payload = self._json_message_dispatcher.build_provision_request(self._provision_request)
            self._log.debug("[Provisioning client] Sending provisioning request %s" % payload)
            client.publish(topic, payload)
        else:
            self._device_config = ProvisioningResponse.build(self._provision_request,
                                                             {'status': 'FAILURE',
                                                              'errorMsg': 'Cannot connect to ThingsBoard!'})
            self._provisioned.set()
            self._log.error("[Provisioning client] Cannot connect to ThingsBoard!, result: %s" % rc)

    async def _on_message(self, _, __, payload, ___, ____):
        decoded_payload = payload.decode("UTF-8")
        self._log.debug("[Provisioning client] Received data from ThingsBoard: %s" % decoded_payload)
        decoded_message = loads(decoded_payload)

        self._device_config = ProvisioningResponse.build(self._provision_request, decoded_message)

        await self._client.disconnect()
        self._provisioned.set()

    async def provision(self):
        await self._client.connect(self._host, self._port)
        await self._provisioned.wait()

        return self._device_config
