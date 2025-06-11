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
from orjson import OPT_NON_STR_KEYS, dumps, loads

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.entities.data.provision_request import ProvisionRequest

PROVISION_REQUEST_TOPIC = "/provision/request"
PROVISION_RESPONSE_TOPIC = "/provision/response"

logger = get_logger(__name__)


class ProvisionClient:
    def __init__(self, host, port, provision_request: 'ProvisionRequest'):
        self._log = logger
        self._stop_event = Event()
        self._host = host
        self._port = port
        self._provision_request = provision_request.to_dict()
        self._client_id = "provision"
        self._client = GMQTTClient(self._client_id)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._provisioned = Event()
        self._device_credentials = None

    def _on_connect(self, client, _, rc, __):
        if rc == 0:
            self._log.debug("[Provisioning client] Connected to ThingsBoard ")
            client.subscribe(PROVISION_RESPONSE_TOPIC)
            provision_request = dumps(self._provision_request, option=OPT_NON_STR_KEYS)
            self._log.debug("[Provisioning client] Sending provisioning request %s" % provision_request)
            client.publish(PROVISION_REQUEST_TOPIC, provision_request)
        else:
            self._device_credentials = None
            self._provisioned.set()
            self._log.error("[Provisioning client] Cannot connect to ThingsBoard!, result: %s" % rc)

    async def _on_message(self, _, __, payload, ___, ____):
        decoded_payload = payload.decode("UTF-8")
        self._log.debug("[Provisioning client] Received data from ThingsBoard: %s" % decoded_payload)
        decoded_message = loads(decoded_payload)
        provision_device_status = decoded_message.get("status")
        if provision_device_status == "SUCCESS":
            self._device_credentials = decoded_message
        else:
            self._log.error("[Provisioning client] Provisioning was unsuccessful with status %s and message: %s" % (
                provision_device_status, decoded_message["errorMsg"]))

        await self._client.disconnect()
        self._provisioned.set()

    async def provision(self):
        await self._client.connect(self._host, self._port)
        await self._provisioned.wait()

        if self._device_credentials:
            return self._device_credentials
