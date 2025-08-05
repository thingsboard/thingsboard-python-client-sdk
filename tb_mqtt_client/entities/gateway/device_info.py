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

import uuid
from dataclasses import dataclass, field


@dataclass()
class DeviceInfo(object):
    device_name: str
    device_profile: str
    original_name: str = field(init=False)
    device_id: uuid.UUID = field(default_factory=uuid.uuid4, init=False)

    _initializing: bool = field(default=True, init=False, repr=False)

    def __post_init__(self):
        self.__setattr__("original_name", self.device_name)
        self._initializing = False

    def __setattr__(self, key, value):
        if not self._initializing:
            raise AttributeError(
                f"Cannot modify attribute '{key}' of frozen DeviceInfo instance."
                "Use rename() method to change device_name.")
        else:
            super().__setattr__(key, value)

    def rename(self, new_name: str):
        if new_name != self.device_name:
            super().__setattr__('device_name', new_name)

    @classmethod
    def from_dict(cls, data: dict) -> 'DeviceInfo':
        original_post_init = cls.__post_init__
        cls.__post_init__ = lambda self: None
        instance = cls(
            device_name=data['device_name'],
            device_profile=data.get('device_profile', 'default')
        )
        instance.__setattr__("device_id", uuid.UUID(data['device_id']))
        if 'original_name' in data:
            instance.__setattr__("original_name", data['original_name'])
        else:
            instance.__setattr__("original_name", instance.device_name)
        instance._initializing = False
        cls.__post_init__ = original_post_init
        return instance

    def to_dict(self) -> dict:
        return {
            "device_name": self.device_name,
            "device_profile": self.device_profile,
            "device_id": str(self.device_id),
            "original_name": self.original_name
        }

    def __str__(self) -> str:
        return (f"DeviceInfo(device_id={self.device_id}, "
                f"device_name={self.device_name}, "
                f"device_profile={self.device_profile}, "
                f"original_name={self.original_name})")

    def __repr__(self) -> str:
        return (f"DeviceInfo(device_id={self.device_id!r}, "
                f"device_name={self.device_name!r}, "
                f"device_profile={self.device_profile!r}, "
                f"original_name={self.original_name!r})")

    def __eq__(self, other):
        if not isinstance(other, DeviceInfo):
            return NotImplemented
        return (self.device_id == other.device_id and
                self.device_name == other.device_name and
                self.device_profile == other.device_profile and
                self.original_name == other.original_name)

    def __hash__(self):
        return hash((self.device_id, self.device_name, self.device_profile, self.original_name))
