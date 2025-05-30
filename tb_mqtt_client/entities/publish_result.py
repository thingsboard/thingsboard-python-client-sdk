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

class PublishResult:
    def __init__(self, topic: str, qos: int, message_id: int, payload_size: int, reason_code: int):
        self.topic = topic
        self.qos = qos
        self.message_id = message_id
        self.payload_size = payload_size
        self.reason_code = reason_code

    def __repr__(self):
        return f"PublishResult(topic={self.topic}, qos={self.qos}, message_id={self.message_id}, payload_size={self.payload_size}, reason_code={self.reason_code})"

    def as_dict(self) -> dict:
        return {
            "topic": self.topic,
            "qos": self.qos,
            "message_id": self.message_id,
            "payload_size": self.payload_size,
            "reason_code": self.reason_code
        }
