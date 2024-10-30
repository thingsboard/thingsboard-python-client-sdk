# Copyright 2024. ThingsBoard
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#  http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging

try:
    from time import monotonic as time
except ImportError:
    from time import time

from tb_device_mqtt import TBDeviceMqttClient, RateLimit, TBSendMethod

GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes"
GATEWAY_TELEMETRY_TOPIC = "v1/gateway/telemetry"
GATEWAY_ATTRIBUTES_REQUEST_TOPIC = "v1/gateway/attributes/request"
GATEWAY_ATTRIBUTES_RESPONSE_TOPIC = "v1/gateway/attributes/response"
GATEWAY_MAIN_TOPIC = "v1/gateway/"
GATEWAY_RPC_TOPIC = "v1/gateway/rpc"
GATEWAY_RPC_RESPONSE_TOPIC = "v1/gateway/rpc/response"
GATEWAY_CLAIMING_TOPIC = "v1/gateway/claim"

log = logging.getLogger("tb_connection")


class TBGatewayAPI:
    pass


class TBGatewayMqttClient(TBDeviceMqttClient):
    def __init__(self, host, port=1883, username=None, password=None, gateway=None, quality_of_service=1, client_id="",
                 messages_rate_limit="DEFAULT_MESSAGES_RATE_LIMIT",
                 telemetry_rate_limit="DEFAULT_TELEMETRY_RATE_LIMIT",
                 telemetry_dp_rate_limit="DEFAULT_TELEMETRY_DP_RATE_LIMIT",
                 device_messages_rate_limit="DEFAULT_MESSAGES_RATE_LIMIT",
                 device_telemetry_rate_limit="DEFAULT_TELEMETRY_RATE_LIMIT",
                 device_telemetry_dp_rate_limit="DEFAULT_TELEMETRY_DP_RATE_LIMIT", **kwargs):
        # Added for compatibility with the old versions
        if kwargs.get('rate_limit') or kwargs.get('dp_rate_limit'):
            messages_rate_limit = messages_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', messages_rate_limit)
            telemetry_rate_limit = telemetry_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', telemetry_rate_limit)
            device_messages_rate_limit = device_messages_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', device_messages_rate_limit)
            device_telemetry_rate_limit = device_telemetry_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', device_telemetry_rate_limit)
            telemetry_dp_rate_limit = telemetry_dp_rate_limit if kwargs.get('dp_rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('dp_rate_limit', telemetry_dp_rate_limit)
            device_telemetry_dp_rate_limit = device_telemetry_dp_rate_limit if kwargs.get('dp_rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('dp_rate_limit', device_telemetry_dp_rate_limit)

        super().__init__(host, port, username, password, quality_of_service, client_id,
                         messages_rate_limit=messages_rate_limit, telemetry_rate_limit=telemetry_rate_limit,
                         telemetry_dp_rate_limit=telemetry_dp_rate_limit)

        self.__device_telemetry_rate_limit, self.__device_telemetry_dp_rate_limit = RateLimit.get_rate_limits_by_host(
            host, device_telemetry_rate_limit, device_telemetry_dp_rate_limit)
        self.__device_messages_rate_limit = RateLimit.get_rate_limit_by_host(host, device_messages_rate_limit)

        self._devices_connected_through_gateway_telemetry_messages_rate_limit = RateLimit(self.__device_telemetry_rate_limit, "Rate limit for devices connected through gateway telemetry messages")
        self._devices_connected_through_gateway_telemetry_datapoints_rate_limit = RateLimit(self.__device_telemetry_dp_rate_limit, "Rate limit for devices connected through gateway telemetry data points")
        self._devices_connected_through_gateway_messages_rate_limit = RateLimit(self.__device_messages_rate_limit, "Rate limit for devices connected through gateway messages")

        self.service_configuration_callback = self.__on_service_configuration
        self.quality_of_service = quality_of_service
        self.__max_sub_id = 0
        self.__sub_dict = {}
        self.__connected_devices = set("*")
        self.devices_server_side_rpc_request_handler = None
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client._on_unsubscribe = self._on_unsubscribe
        self._gw_subscriptions = {}
        self.gateway = gateway

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        super()._on_connect(client, userdata, flags, result_code, *extra_params)
        if result_code == 0:
            gateway_attributes_topic_sub_id = int(self._subscribe_to_topic(GATEWAY_ATTRIBUTES_TOPIC, qos=1)[1])
            self._add_or_delete_subscription(GATEWAY_ATTRIBUTES_TOPIC, gateway_attributes_topic_sub_id)

            gateway_attributes_resp_sub_id = int(self._subscribe_to_topic(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, qos=1)[1])
            self._add_or_delete_subscription(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, gateway_attributes_resp_sub_id)

            gateway_rpc_topic_sub_id = int(self._subscribe_to_topic(GATEWAY_RPC_TOPIC, qos=1)[1])
            self._add_or_delete_subscription(GATEWAY_RPC_TOPIC, gateway_rpc_topic_sub_id)

    def _on_subscribe(self, client, userdata, mid, reasoncodes, properties=None):
        subscription = self._gw_subscriptions.get(mid)
        if subscription is not None:
            if mid == 128:
                self._delete_subscription(subscription, mid)
            else:
                log.debug("Service subscription to topic %s - successfully completed.", subscription)
                del self._gw_subscriptions[mid]

    def _delete_subscription(self, topic, subscription_id):
        log.error("Service subscription to topic %s - failed.", topic)
        if subscription_id in self._gw_subscriptions:
            del self._gw_subscriptions[subscription_id]

    def _add_or_delete_subscription(self, topic, subscription_id):
        if subscription_id == 128:
            self._delete_subscription(topic, subscription_id)
        else:
            self._gw_subscriptions[subscription_id] = topic

    @staticmethod
    def _on_unsubscribe(*args):
        log.debug(args)

    def get_subscriptions_in_progress(self):
        return True if self._gw_subscriptions else False

    def _on_message(self, client, userdata, message):
        content = self._decode(message)
        super()._on_decoded_message(content, message)
        self._on_decoded_message(content, message)

    def _on_decoded_message(self, content, message, **kwargs):
        if message.topic.startswith(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC):
            with self._lock:
                req_id = content["id"]
                self._devices_connected_through_gateway_messages_rate_limit.increase_rate_limit_counter(1)
                # pop callback and use it
                if self._attr_request_dict[req_id]:
                    callback = self._attr_request_dict.pop(req_id)
                    if isinstance(callback, tuple):
                        callback[0](content, None, callback[1])
                    else:
                        callback(content, None)
                else:
                    log.error("Unable to find callback to process attributes response from TB")
        elif message.topic == GATEWAY_ATTRIBUTES_TOPIC:
            with self._lock:
                # callbacks for everything
                if self.__sub_dict.get("*|*"):
                    for device in self.__sub_dict["*|*"]:
                        self.__sub_dict["*|*"][device](content)
                # callbacks for device. in this case callback executes for all attributes in message
                if content.get("device") is None:
                    return
                self._devices_connected_through_gateway_messages_rate_limit.increase_rate_limit_counter(1)
                target = content["device"] + "|*"
                if self.__sub_dict.get(target):
                    for device in self.__sub_dict[target]:
                        self.__sub_dict[target][device](content)
                # callback for atr. in this case callback executes for all attributes in message
                targets = [content["device"] + "|" + attribute for attribute in content["data"]]
                for target in targets:
                    if self.__sub_dict.get(target):
                        for device in self.__sub_dict[target]:
                            self.__sub_dict[target][device](content)
        elif message.topic == GATEWAY_RPC_TOPIC:
            self._devices_connected_through_gateway_messages_rate_limit.increase_rate_limit_counter(1)
            if self.devices_server_side_rpc_request_handler:
                self.devices_server_side_rpc_request_handler(self, content)

    def __request_attributes(self, device, keys, callback, type_is_client=False):
        if not keys:
            log.error("There are no keys to request")
            return False

        attr_request_number = self._add_attr_request_callback(callback)
        msg = {"keys": keys,
               "device": device,
               "client": type_is_client,
               "id": attr_request_number}
        info = self._send_device_request(TBSendMethod.PUBLISH, device, topic=GATEWAY_ATTRIBUTES_REQUEST_TOPIC, data=msg,
                                         qos=1)
        self.add_attrs_request_timeout(attr_request_number, int(time()) + 20)
        return info

    def _send_device_request(self, _type, device_name, **kwargs):
        if _type == TBSendMethod.PUBLISH:
            device_msg_rate_limit = self._devices_connected_through_gateway_messages_rate_limit
            device_dp_rate_limit = self.EMPTY_RATE_LIMIT
            if kwargs.get('topic') == GATEWAY_TELEMETRY_TOPIC:
                device_msg_rate_limit = self._devices_connected_through_gateway_telemetry_messages_rate_limit
                device_dp_rate_limit = self._devices_connected_through_gateway_telemetry_datapoints_rate_limit
            info = self._publish_data(**kwargs, device=device_name,
                                      msg_rate_limit=device_msg_rate_limit,
                                      dp_rate_limit=device_dp_rate_limit)
            return info

    def gw_request_shared_attributes(self, device_name, keys, callback):
        return self.__request_attributes(device_name, keys, callback, False)

    def gw_request_client_attributes(self, device_name, keys, callback):
        return self.__request_attributes(device_name, keys, callback, True)

    def gw_send_attributes(self, device, attributes, quality_of_service=1):
        return self._send_device_request(TBSendMethod.PUBLISH,
                                         device,
                                         topic=GATEWAY_ATTRIBUTES_TOPIC,
                                         data={device: attributes},
                                         qos=quality_of_service)

    def gw_send_telemetry(self, device, telemetry, quality_of_service=1):
        if not isinstance(telemetry, list):
            telemetry = [telemetry]

        return self._send_device_request(TBSendMethod.PUBLISH,
                                         device,
                                         topic=GATEWAY_TELEMETRY_TOPIC,
                                         data={device: telemetry},
                                         qos=quality_of_service)

    def gw_connect_device(self, device_name, device_type="default"):
        info = self._send_device_request(TBSendMethod.PUBLISH, device_name, topic=GATEWAY_MAIN_TOPIC + "connect",
                                         data={"device": device_name, "type": device_type},
                                         qos=self.quality_of_service)

        self.__connected_devices.add(device_name)

        log.debug("Connected device %s", device_name)
        return info

    def gw_disconnect_device(self, device_name):
        info = self._send_device_request(TBSendMethod.PUBLISH, device_name, topic=GATEWAY_MAIN_TOPIC + "disconnect",
                                         data={"device": device_name}, qos=self.quality_of_service)

        if device_name in self.__connected_devices:
            self.__connected_devices.remove(device_name)

        log.debug("Disconnected device %s", device_name)
        return info

    def gw_subscribe_to_all_attributes(self, callback):
        return self.gw_subscribe_to_attribute("*", "*", callback)

    def gw_subscribe_to_all_device_attributes(self, device, callback):
        return self.gw_subscribe_to_attribute(device, "*", callback)

    def gw_subscribe_to_attribute(self, device, attribute, callback):
        if device not in self.__connected_devices:
            log.error("Device %s is not connected", device)
            return False

        with self._lock:
            self.__max_sub_id += 1
            key = device + "|" + attribute
            if key not in self.__sub_dict:
                self.__sub_dict.update({key: {device: callback}})
            else:
                self.__sub_dict[key].update({device: callback})

            log.info("Subscribed to %s with id %i for device %s", key, self.__max_sub_id, device)
            return self.__max_sub_id

    def gw_unsubscribe(self, subscription_id):
        with self._lock:
            for attribute in self.__sub_dict:
                if self.__sub_dict[attribute].get(subscription_id):
                    del self.__sub_dict[attribute][subscription_id]
                    log.info("Unsubscribed from %s, subscription id %r", attribute, subscription_id)
            if subscription_id == '*':
                self.__sub_dict = {}

    def gw_set_server_side_rpc_request_handler(self, handler):
        self.devices_server_side_rpc_request_handler = handler

    def gw_send_rpc_reply(self, device, req_id, resp, quality_of_service=None):
        if quality_of_service is None:
            quality_of_service = self.quality_of_service
        if quality_of_service not in (0, 1):
            log.error("Quality of service (qos) value must be 0 or 1")
            return None

        info = self._send_device_request(TBSendMethod.PUBLISH, device, topic=GATEWAY_RPC_TOPIC,
                                         data={"device": device, "id": req_id, "data": resp},
                                         qos=quality_of_service)
        return info

    def gw_claim(self, device_name, secret_key, duration, claiming_request=None):
        if claiming_request is None:
            claiming_request = {
                device_name: {
                    "secretKey": secret_key,
                    "durationMs": duration
                }
            }

        info = self._send_device_request(TBSendMethod.PUBLISH, device_name, topic=GATEWAY_CLAIMING_TOPIC,
                                         data=claiming_request, qos=self.quality_of_service)
        return info

    def __on_service_configuration(self, _, response, *args, **kwargs):
        if "error" in response:
            log.warning("Timeout while waiting for service configuration!, session will use default configuration.")
            self.rate_limits_received = True
            return
        service_config = response
        gateway_devices_rate_limit_config = service_config.pop('gatewayRateLimits', {})
        gateway_device_itself_rate_limit_config = service_config.pop('rateLimits', {})

        if gateway_devices_rate_limit_config.get("messages"):
            self._devices_connected_through_gateway_messages_rate_limit.set_limit(
                gateway_devices_rate_limit_config.get("messages"))
        else:
            self._devices_connected_through_gateway_messages_rate_limit.set_limit('0:0,')

        if gateway_devices_rate_limit_config.get('telemetryMessages'):
            self._devices_connected_through_gateway_telemetry_messages_rate_limit.set_limit(
                gateway_devices_rate_limit_config.get('telemetryMessages'))
        else:
            self._devices_connected_through_gateway_telemetry_messages_rate_limit.set_limit('0:0,')

        if gateway_devices_rate_limit_config.get('telemetryDataPoints'):
            self._devices_connected_through_gateway_telemetry_datapoints_rate_limit.set_limit(
                gateway_devices_rate_limit_config.get('telemetryDataPoints'))
        else:
            self._devices_connected_through_gateway_telemetry_datapoints_rate_limit.set_limit('0:0,')

        super().on_service_configuration(_, {'rateLimit': gateway_device_itself_rate_limit_config, **service_config}, *args, **kwargs)
