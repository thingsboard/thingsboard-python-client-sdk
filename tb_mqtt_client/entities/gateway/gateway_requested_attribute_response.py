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
from typing import Dict, Any, List, Optional

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry


@dataclass(slots=True, frozen=True)
class GatewayRequestedAttributeResponse:

    device_name: str
    request_id: int
    shared: Optional[List[AttributeEntry]] = None
    client: Optional[List[AttributeEntry]] = None

    def __repr__(self):
        return f"GatewayRequestedAttributeResponse(device_name={self.device_name},request_id={self.request_id}, shared={self.shared}, client={self.client})"

    def __getitem__(self, item):
        """
        Allows access to values using dictionary-like syntax.
        """
        if self.shared is not None:
            for entry in self.shared:
                if entry.key == item:
                    return entry.value
        if self.client is not None:
            for entry in self.client:
                if entry.key == item:
                    return entry.value
        raise KeyError(f"Key '{item}' not found in shared or client attributes.")

    def shared_keys(self):
        return [entry.key for entry in self.shared]

    def client_keys(self):
        return [entry.key for entry in self.client]

    def get_shared(self, key: str, default=None):
        """
        Get the value of a shared attribute by key.
        :param key: The key of the shared attribute.
        :param default: Default value if the key is not found.
        :return: Value of the shared attribute or default.
        """
        if self.shared is not None:
            for entry in self.shared:
                if entry.key == key:
                    return entry.value
        return default

    def get_client(self, key: str, default=None):
        """
        Get the value of a client attribute by key.
        :param key: The key of the client attribute.
        :param default: Default value if the key is not found.
        :return: Value of the client attribute or default.
        """
        if self.client is not None:
            for entry in self.client:
                if entry.key == key:
                    return entry.value
        return default

    def as_dict(self) -> Dict[str, Any]:
        """
        Convert the GatewayRequestedAttributeResponse to a dictionary format.
        :return: Dictionary representation of the response.
        """
        return {
            'shared': [entry.as_dict() for entry in self.shared if self.shared is not None],
            'client': [entry.as_dict() for entry in self.client if self.client is not None],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GatewayRequestedAttributeResponse':
        """
        Deserialize dictionary into GatewayRequestedAttributeResponse object.
        :param data: Dictionary containing 'device' with device name, 'shared' and 'client' attributes.
        :return: GatewayRequestedAttributeResponse instance.
        """
        request_id = data.get('request_id', -1)
        device_name = data.get('device', '')
        shared = [AttributeEntry(k, v) for k, v in data.get('shared', {}).items()]
        client = [AttributeEntry(k, v) for k, v in data.get('client', {}).items()]
        return cls(device_name=device_name, shared=shared, client=client, request_id=request_id)
