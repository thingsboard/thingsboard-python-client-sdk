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
        self._consecutive_quota_exceeded = 0
        self._last_quota_exceeded = datetime.now(UTC)
        self._max_backoff_seconds = 3600  # 1 hour maximum backoff

    def notify_quota_exceeded(self, delay_seconds: Optional[int] = None):
        now = datetime.now(UTC)
        # If we've had a quota exceeded event in the last 60 seconds, increment the counter
        if (now - self._last_quota_exceeded).total_seconds() < 60:
            self._consecutive_quota_exceeded += 1
        else:
            # Reset counter if it's been more than 60 seconds since the last quota exceeded event
            self._consecutive_quota_exceeded = 1

        self._last_quota_exceeded = now

        # Apply exponential backoff based on consecutive quota exceeded events
        if delay_seconds is None:
            # Start with default duration and apply exponential backoff
            backoff_factor = min(2 ** (self._consecutive_quota_exceeded - 1), 10)
            delay_seconds = int(self._default_pause_duration.total_seconds() * backoff_factor)
            # Cap at max backoff
            delay_seconds = min(delay_seconds, self._max_backoff_seconds)

        logger.warning("Applying backpressure for %d seconds (consecutive quota exceeded: %d)", 
                      delay_seconds, self._consecutive_quota_exceeded)

        duration = timedelta(seconds=delay_seconds)
        self._pause_until = now + duration

    def notify_disconnect(self, delay_seconds: Optional[int] = None):
        if delay_seconds is None:
            delay_seconds = int(self._default_pause_duration.total_seconds())

        duration = timedelta(seconds=delay_seconds)
        self._pause_until = datetime.now(UTC) + duration
        logger.debug("Pausing publishing for %d seconds due to disconnect", delay_seconds)

    def should_pause(self) -> bool:
        if self._pause_until is None:
            return False

        now = datetime.now(UTC)
        if now < self._pause_until:
            remaining = (self._pause_until - now).total_seconds()
            if remaining > 10:  # Only log if more than 10 seconds remaining
                logger.debug("Backpressure active: pausing publishing for %.1f more seconds", remaining)
            return True

        # Reset pause state
        self._pause_until = None
        logger.info("Backpressure released, resuming publishing")
        return False

    def pause_for(self, seconds: int):
        self._pause_until = datetime.now(UTC) + timedelta(seconds=seconds)
        logger.info("Manually pausing publishing for %d seconds", seconds)

    def clear(self):
        if self._pause_until is not None:
            logger.info("Clearing backpressure pause")
        self._pause_until = None
        self._consecutive_quota_exceeded = 0
