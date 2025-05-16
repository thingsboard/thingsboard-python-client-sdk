#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#


from typing import Any, Optional
from orjson import dumps


class DataEntry:
    def __init__(self, key: str, value: Any, ts: Optional[int] = None):
        self.__key = key
        self.__value = value
        self.__ts = ts
        self.__size = self.__estimate_size()

    def __estimate_size(self) -> int:
        if self.ts is not None:
            return len(dumps({"ts": self.ts, "values": {self.key: self.value}}))
        else:
            return len(dumps({self.key: self.value}))

    @property
    def size(self) -> int:
        return self.__size

    @property
    def key(self) -> str:
        return self.__key

    @key.setter
    def key(self, value: str):
        self.__key = value
        self.__size = self.__estimate_size()

    @property
    def value(self) -> Any:
        return self.__value

    @value.setter
    def value(self, value: Any):
        self.__value = value
        self.__size = self.__estimate_size()

    @property
    def ts(self) -> Optional[int]:
        return self.__ts

    @ts.setter
    def ts(self, value: Optional[int]):
        self.__ts = value
        self.__size = self.__estimate_size()
