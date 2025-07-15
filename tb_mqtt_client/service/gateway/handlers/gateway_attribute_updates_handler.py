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

from tb_mqtt_client.service.gateway.device_manager import DeviceManager
from tb_mqtt_client.service.gateway.event_dispatcher import EventDispatcher
from tb_mqtt_client.service.gateway.message_adapter import GatewayMessageAdapter


class GatewayAttributeUpdatesHandler:
    """Handles shared attribute updates for devices connected to a gateway."""
    def __init__(self,
                 event_dispatcher: EventDispatcher,
                 message_adapter: GatewayMessageAdapter,
                 device_manager: DeviceManager):
        self.event_dispatcher = event_dispatcher
        self.message_adapter = message_adapter
        self.device_manager = device_manager

    def handle(self, topic: str, payload: bytes):
        """
        Handles the gateway attribute update event by dispatching the attribute update
        """
        data = self.message_adapter.deserialize_to_dict(payload)
        gateway_attribute_update = self.message_adapter.parse_attribute_update(data)
        device_session = self.device_manager.get_by_name(gateway_attribute_update.device_name)
        if device_session:
            gateway_attribute_update.set_device_session(device_session)
        self.event_dispatcher.dispatch(gateway_attribute_update)
