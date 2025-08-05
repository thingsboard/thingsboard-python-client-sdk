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

from tb_mqtt_client.constants import mqtt_topics


def test_device_topic_builders():
    assert mqtt_topics.build_device_attributes_request_topic(42) == "v1/devices/me/attributes/request/42"
    assert mqtt_topics.build_device_rpc_request_topic(99) == "v1/devices/me/rpc/request/99"
    assert mqtt_topics.build_device_rpc_response_topic(99) == "v1/devices/me/rpc/response/99"
