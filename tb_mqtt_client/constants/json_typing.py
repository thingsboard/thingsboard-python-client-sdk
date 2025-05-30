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

from typing import Union, List, Dict

JSONPrimitive = Union[str, int, float, bool, None]
JSONCompatibleType = Union[JSONPrimitive, List["JSONType"], Dict[str, "JSONType"]]

def validate_json_compatibility(value: object) -> None:
    """
    Validates that the input value is fully JSON-compatible in structure and type.
    Raises a ValueError on the first incompatible type encountered.
    """
    stack = [(value, "$")]
    basic_types = (str, int, float, bool, type(None))

    while stack:
        current, path = stack.pop()
        t = type(current)

        if t in basic_types:
            continue
        if t is list:
            for idx, item in enumerate(current):  # type: ignore[arg-type]
                stack.append((item, f"{path}[{idx}]"))
        elif t is dict:
            for k, v in current.items():  # type: ignore[union-attr]
                if type(k) is not str:
                    raise ValueError(f"Invalid JSON key at {path}: expected str, got {type(k).__name__} ({k!r})")
                stack.append((v, f"{path}.{k}"))
        else:
            raise ValueError(f"Invalid JSON value at {path}: unsupported - {type(current).__name__} ({current!r})")