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
from typing import Optional, Dict, Any


@dataclass(slots=True, frozen=True)
class ClaimRequest:
    """
    Represents a device claim request, as per ThingsBoard MQTT API.
    Optionally includes a secret key and a duration (in seconds) for the claim.
    Does not include request_id, as it's not required for this operation.
    """
    secret_key: Optional[str] = None
    duration: Optional[int] = None  # seconds

    def __new__(cls, *args, **kwargs):
        raise TypeError("Direct instantiation of ClaimRequest is not allowed. Use 'ClaimRequest.build(...)'.")

    def __repr__(self) -> str:
        return f"ClaimRequest(secret_key={self.secret_key}, duration={self.duration})"

    @classmethod
    def build(cls, secret_key: str, duration: int = 60000) -> 'ClaimRequest':
        """
        Safely construct a new ClaimRequest instance.
        """
        if not isinstance(secret_key, str):
            raise ValueError("Secret key must be a string")
        if not isinstance(duration, int) or duration < 0:
            raise ValueError("Duration must be a non-negative integer representing seconds")
        self = object.__new__(cls)
        object.__setattr__(self, 'secret_key', secret_key)
        object.__setattr__(self, 'duration', duration)
        return self

    def to_payload_format(self) -> Dict[str, Any]:
        """
        Convert the claim request to the expected MQTT JSON payload format.
        """
        payload = {}
        if self.secret_key is not None:
            payload["secretKey"] = self.secret_key
        if self.duration is not None:
            payload["durationMs"] = int(self.duration * 1000)
        return payload