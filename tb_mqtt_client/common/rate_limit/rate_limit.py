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

import os
import logging
from asyncio import Lock
from time import monotonic

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5

try:
    DEFAULT_RATE_LIMIT_PERCENTAGE = int(os.getenv('TB_DEFAULT_RATE_LIMIT_PERCENTAGE', 80))
except ValueError:
    logger.warning("Invalid TB_DEFAULT_RATE_LIMIT_PERCENTAGE, falling back to 80%%")
    DEFAULT_RATE_LIMIT_PERCENTAGE = 80


class GreedyTokenBucket:
    def __init__(self, capacity, duration_sec):
        self.capacity = float(capacity)
        self.duration = float(duration_sec)
        self.tokens = float(capacity)
        self.last_updated = monotonic()

    def refill(self):
        now = monotonic()
        elapsed = now - self.last_updated
        rate = self.capacity / self.duration
        self.tokens = min(self.capacity, self.tokens + elapsed * rate)
        self.last_updated = now

    def can_consume(self, amount=1):
        self.refill()
        return round(self.tokens, 6) >= round(amount, 6)

    def consume(self, amount=1):
        self.refill()
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False

    def get_remaining_tokens(self):
        self.refill()
        return self.tokens


class RateLimit:
    def __init__(self, rate_limit: str, name: str = None, percentage: int = DEFAULT_RATE_LIMIT_PERCENTAGE):
        self.name = name
        self.percentage = percentage
        self._no_limit = False
        self._rate_buckets = {}
        self._lock = Lock()
        self._minimal_timeout = DEFAULT_TIMEOUT
        self._minimal_limit = float('inf')
        self.__reached_index = 0
        self.__reached_index_time = 0

        self._parse_string(rate_limit)

    def _parse_string(self, rate_limit: str):
        if not rate_limit or rate_limit.strip().removesuffix(',') in ("0:0", ""):
            self._no_limit = True
            return

        entries = rate_limit.replace(";", ",").split(",")
        for entry in entries:
            if not entry.strip():
                continue
            try:
                limit_str, dur_str = entry.strip().split(":")
                limit = int(int(limit_str) * self.percentage / 100)
                duration = int(dur_str)

                bucket = GreedyTokenBucket(limit, duration)
                self._rate_buckets[duration] = bucket
                self._minimal_limit = min(self._minimal_limit, limit)
                self._minimal_timeout = min(self._minimal_timeout, duration + 1)
            except Exception as e:
                logger.warning("Invalid rate limit format '%s': %s", entry, e)

        self._no_limit = not bool(self._rate_buckets)

    async def check_limit_reached(self, amount=1):
        if self._no_limit:
            return False

        async with self._lock:
            result = False
            for dur, bucket in self._rate_buckets.items():
                bucket.refill()
                if not result and bucket.tokens < amount:
                    result = (bucket.capacity, dur)
            return result

    async def refill(self):
        """Force refill of all token buckets without consuming any tokens."""
        if self._no_limit:
            return
        async with self._lock:
            for bucket in self._rate_buckets.values():
                bucket.refill()

    async def try_consume(self, amount=1):
        """
        Try to consume tokens from all buckets.
        Returns True if all buckets had enough tokens and they were consumed.
        Returns False if any bucket didn't have enough tokens.
        """
        if self._no_limit:
            return None

        async with self._lock:
            for bucket in self._rate_buckets.values():
                bucket.refill()
                if bucket.tokens < amount:
                    return bucket.capacity, bucket.duration

            for bucket in self._rate_buckets.values():
                bucket.tokens -= amount

            return None

    async def consume(self, amount=1):
        if self._no_limit:
            return
        async with self._lock:
            for bucket in self._rate_buckets.values():
                bucket.consume(amount)

    @property
    def minimal_limit(self):
        return self._minimal_limit if self.has_limit() else 0

    @property
    def minimal_timeout(self):
        return self._minimal_timeout if self.has_limit() else 0

    def has_limit(self):
        return not self._no_limit

    async def reach_limit(self):
        if self._no_limit:
            return None

        async with self._lock:
            durations = sorted(self._rate_buckets.keys())
            now = monotonic()

            if self.__reached_index_time >= now - self._rate_buckets[durations[-1]].duration:
                self.__reached_index = 0
                self.__reached_index_time = now

            if self.__reached_index >= len(durations):
                self.__reached_index = 0
                self.__reached_index_time = now

            dur = durations[self.__reached_index]
            self._rate_buckets[dur].tokens = 0.0
            self.__reached_index += 1

            logger.info("Rate limit reached for \"%s\". Cooldown for %s seconds", self.name, dur)
            return self.__reached_index, self.__reached_index_time, dur

    def to_dict(self):
        return {
            "name": self.name,
            "percentage": self.percentage,
            "no_limit": self._no_limit,
            "rateLimits": {
                str(dur): {
                    "capacity": b.capacity,
                    "tokens": b.get_remaining_tokens(),
                    "last_updated": b.last_updated
                } for dur, b in self._rate_buckets.items()
            }
        }

    async def set_limit(self, rate_limit: str, percentage: int = DEFAULT_RATE_LIMIT_PERCENTAGE):
        async with self._lock:
            self._rate_buckets.clear()
            self._minimal_timeout = DEFAULT_TIMEOUT
            self._minimal_limit = float('inf')
            self.percentage = percentage
            self._parse_string(rate_limit)
