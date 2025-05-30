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

from typing import Any, Optional

from tb_mqtt_client.constants.json_typing import JSONCompatibleType
from tb_mqtt_client.entities.data.data_entry import DataEntry


class TimeseriesEntry(DataEntry):
    def __init__(self, key: str, value: JSONCompatibleType, ts: Optional[int] = None):
        super().__init__(key, value, ts)

    def __repr__(self):
        return f"TelemetryEntry(key={self.key}, value={self.value}, ts={self.ts})"

    def as_dict(self) -> dict:
        result = {
            "key": self.key,
            "value": self.value
        }
        if self.ts is not None:
            result["ts"] = self.ts
        return result

    def __eq__(self, other):
        if not isinstance(other, TimeseriesEntry):
            return False
        return self.key == other.key and self.value == other.value and self.ts == other.ts
