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

from dataclasses import dataclass
from typing import Dict, Any, List

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.gateway.base_gateway_event import BaseGatewayEvent
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType


@dataclass(slots=True)
class AttributeUpdate(BaseGatewayEvent):
    entries: List[AttributeEntry]
    event_type: GatewayEventType = GatewayEventType.DEVICE_ATTRIBUTE_UPDATE

    def __repr__(self):
        return f"AttributeUpdate(entries={self.entries})"

    def get(self, key: str, default=None):
        for entry in self.entries:
            if entry.key == key:
                return entry.value
        return default

    def keys(self):
        return [entry.key for entry in self.entries]

    def values(self):
        return [entry.value for entry in self.entries]

    def items(self):
        return [(entry.key, entry.value) for entry in self.entries]

    def as_dict(self) -> Dict[str, Any]:
        return {entry.key: entry.value for entry in self.entries}

    @classmethod
    def _deserialize_from_dict(cls, data: Dict[str, Any]) -> 'AttributeUpdate':
        """
        Deserialize dictionary into AttributeUpdate object.
        :param data: Dictionary of attribute key-value pairs.
        :return: AttributeUpdate instance.
        """
        entries = [AttributeEntry(k, v) for k, v in data.items()]
        return cls(entries=entries)
