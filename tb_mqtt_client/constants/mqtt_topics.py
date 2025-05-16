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


WILDCARD = "+"
REQUEST_TOPIC_SUFFIX = "/request"
RESPONSE_TOPIC_SUFFIX = "/response"
# V1 Topics for Device API
DEVICE_TELEMETRY_TOPIC = "v1/devices/me/telemetry"
DEVICE_ATTRIBUTES_TOPIC = "v1/devices/me/attributes"
DEVICE_ATTRIBUTES_REQUEST_TOPIC = DEVICE_ATTRIBUTES_TOPIC + REQUEST_TOPIC_SUFFIX + "/" + "{request_id}"
DEVICE_ATTRIBUTES_RESPONSE_TOPIC = DEVICE_ATTRIBUTES_TOPIC + RESPONSE_TOPIC_SUFFIX + "/" + WILDCARD
DEVICE_RPC_TOPIC = "v1/devices/me/rpc"
# Device RPC topics
DEVICE_RPC_REQUEST_TOPIC = DEVICE_RPC_TOPIC + REQUEST_TOPIC_SUFFIX + "/"
DEVICE_RPC_RESPONSE_TOPIC = DEVICE_RPC_TOPIC + RESPONSE_TOPIC_SUFFIX + "/"
DEVICE_RPC_REQUEST_TOPIC_FOR_SUBSCRIPTION = DEVICE_RPC_TOPIC + REQUEST_TOPIC_SUFFIX + "/" + WILDCARD
DEVICE_RPC_RESPONSE_TOPIC_FOR_SUBSCRIPTION = DEVICE_RPC_TOPIC + RESPONSE_TOPIC_SUFFIX + "/" + WILDCARD

# V1 Topics for Gateway API
BASE_GATEWAY_TOPIC = "v1/gateway"
GATEWAY_CONNECT_TOPIC = BASE_GATEWAY_TOPIC + "/connect"
GATEWAY_DISCONNECT_TOPIC = BASE_GATEWAY_TOPIC + "disconnect"
GATEWAY_TELEMETRY_TOPIC = BASE_GATEWAY_TOPIC + "telemetry"
GATEWAY_ATTRIBUTES_TOPIC = BASE_GATEWAY_TOPIC + "attributes"
GATEWAY_ATTRIBUTES_REQUEST_TOPIC = GATEWAY_ATTRIBUTES_TOPIC + REQUEST_TOPIC_SUFFIX
GATEWAY_ATTRIBUTES_RESPONSE_TOPIC = GATEWAY_ATTRIBUTES_TOPIC + RESPONSE_TOPIC_SUFFIX
GATEWAY_RPC_TOPIC = BASE_GATEWAY_TOPIC + "rpc"

# Topic Builders


def build_device_attributes_request_topic(request_id: int) -> str:
    return DEVICE_ATTRIBUTES_REQUEST_TOPIC.format(request_id=request_id)


def build_device_rpc_request_topic(request_id: int) -> str:
    return DEVICE_RPC_REQUEST_TOPIC + str(request_id)


def build_device_rpc_response_topic(request_id: int) -> str:
    return DEVICE_RPC_RESPONSE_TOPIC + str(request_id)


def build_gateway_device_telemetry_topic() -> str:
    return GATEWAY_TELEMETRY_TOPIC


def build_gateway_device_attributes_topic() -> str:
    return GATEWAY_ATTRIBUTES_TOPIC


def build_gateway_rpc_topic() -> str:
    return GATEWAY_RPC_TOPIC
