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

from typing import List, Union

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


class GatewayAttributeUpdate(AttributeUpdate, BaseGatewayEvent):
    """
    Represents an attribute update event for a device connected to a gateway.
    This event is used to notify about changes in device shared attributes.
    """

    def __init__(self, device_name: str,
                 attribute_update: Union[AttributeUpdate, List[AttributeEntry], AttributeEntry]):
        super().__init__(GatewayEventType.DEVICE_ATTRIBUTE_UPDATE)
        if isinstance(attribute_update, list) and all(isinstance(entry, AttributeEntry) for entry in attribute_update):
            attribute_update = AttributeUpdate(entries=attribute_update)
        elif isinstance(attribute_update, AttributeEntry):
            attribute_update = AttributeUpdate(entries=[attribute_update])
        elif not isinstance(attribute_update, AttributeUpdate):
            raise TypeError(
                "attribute_update must be an instance of AttributeUpdate, "
                "list of AttributeEntry, or a single AttributeEntry.")
        self.device_name = device_name
        self.entries = attribute_update.entries
        self.attribute_update = attribute_update

    def __str__(self) -> str:
        return f"GatewayAttributeUpdate(device_name={self.device_name}, attribute_update={self.attribute_update})"
