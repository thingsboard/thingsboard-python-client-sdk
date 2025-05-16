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


from datetime import datetime, timedelta, UTC
from typing import Optional

from tb_mqtt_client.common.logging_utils import get_logger

logger = get_logger(__name__)


class BackpressureController:
    def __init__(self):
        self._pause_until: Optional[datetime] = None
        self._default_pause_duration = timedelta(seconds=10)

    def notify_quota_exceeded(self, delay_seconds: Optional[int] = None):
        duration = timedelta(seconds=delay_seconds) if delay_seconds else self._default_pause_duration
        self._pause_until = datetime.now(UTC) + duration

    def notify_disconnect(self, delay_seconds: Optional[int] = None):
        self.notify_quota_exceeded(delay_seconds)

    def should_pause(self) -> bool:
        if self._pause_until is None:
            return False
        if datetime.now(UTC) < self._pause_until:
            return True
        self._pause_until = None
        return False

    def pause_for(self, seconds: int):
        self._pause_until = datetime.now(UTC) + timedelta(seconds=seconds)

    def clear(self):
        self._pause_until = None
