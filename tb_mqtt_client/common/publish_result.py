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
from typing import List


class PublishResult:
    def __init__(self, topic: str, qos: int, message_id: int, payload_size: int, reason_code: int,
                 datapoints_count: int = 0):
        self.topic = topic
        self.qos = qos
        self.message_id = message_id
        self.payload_size = payload_size
        self.reason_code = reason_code
        self.datapoints_count = datapoints_count

    def __repr__(self):
        return (f"PublishResult(topic={self.topic}, "
                f"qos={self.qos}, "
                f"message_id={self.message_id}, "
                f"payload_size={self.payload_size}, "
                f"reason_code={self.reason_code}, "
                f"datapoints_count={self.datapoints_count})")

    def __eq__(self, other):
        if not isinstance(other, PublishResult):
            return NotImplemented
        return (self.topic == other.topic and
                self.qos == other.qos and
                self.message_id == other.message_id and
                self.payload_size == other.payload_size and
                self.reason_code == other.reason_code and
                self.datapoints_count == other.datapoints_count)

    def as_dict(self) -> dict:
        return {
            "topic": self.topic,
            "qos": self.qos,
            "message_id": self.message_id,
            "payload_size": self.payload_size,
            "reason_code": self.reason_code,
            "datapoints_count": self.datapoints_count
        }

    def is_successful(self) -> bool:
        """
        Check if the publish operation was successful based on the reason code.
        """
        return self.reason_code == 0


    @staticmethod
    def merge(results: List['PublishResult']) -> 'PublishResult':
        if not results:
            raise ValueError("No publish results to merge.")

        topic = results[0].topic
        qos = results[0].qos
        message_id = -1
        reason_code = 0 if all(r.reason_code == 0 for r in results) else -1
        payload_size = sum(r.payload_size for r in results)
        datapoints_count = sum(r.datapoints_count for r in results)

        return PublishResult(
            topic=topic,
            qos=qos,
            message_id=message_id,
            payload_size=payload_size,
            reason_code=reason_code,
            datapoints_count=datapoints_count
        )
