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

from tb_mqtt_client.constants.json_typing import JSONCompatibleType
from tb_mqtt_client.entities.data.data_entry import DataEntry


class AttributeEntry(DataEntry):
    def __init__(self, key: str, value: JSONCompatibleType):
        super().__init__(key, value)

    def __repr__(self):
        return f"AttributeEntry(key={self.key}, value={self.value})"

    def as_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value
        }

    def __eq__(self, other):
        if not isinstance(other, AttributeEntry):
            return False
        return self.key == other.key and self.value == other.value
