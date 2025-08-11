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

from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit


@dataclass()
class RateLimiter:
    message_rate_limit: RateLimit
    telemetry_message_rate_limit: RateLimit
    telemetry_datapoints_rate_limit: RateLimit

    def values(self):
        return [self.message_rate_limit, self.telemetry_message_rate_limit, self.telemetry_datapoints_rate_limit]

    def __repr__(self):
        return f"RateLimiter(message_rate_limit={self.message_rate_limit}, " \
               f"telemetry_message_rate_limit={self.telemetry_message_rate_limit}, " \
               f"telemetry_datapoints_rate_limit={self.telemetry_datapoints_rate_limit})"
