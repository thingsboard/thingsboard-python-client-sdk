#      Copyright 2020. ThingsBoard
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
#

import logging
import time
from json import dumps
from tb_device_mqtt import TBDeviceMqttClient, DEVICE_TS_KV_VALIDATOR, KV_VALIDATOR


GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes"
GATEWAY_ATTRIBUTES_REQUEST_TOPIC = "v1/gateway/attributes/request"
GATEWAY_ATTRIBUTES_RESPONSE_TOPIC = "v1/gateway/attributes/response"
GATEWAY_MAIN_TOPIC = "v1/gateway/"
GATEWAY_RPC_TOPIC = "v1/gateway/rpc"
GATEWAY_RPC_RESPONSE_TOPIC = "v1/gateway/rpc/response"
GATEWAY_CLAIMING_TOPIC = "v1/gateway/claim"

log = logging.getLogger(__name__)


class TBGatewayAPI:
    pass


class TBGatewayMqttClient(TBDeviceMqttClient):
    def __init__(self, host, token=None, port=1883, gateway=None, quality_of_service=1):
        super().__init__(host, token, port, quality_of_service)
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
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_ATTRIBUTES_TOPIC, qos=1)[1])] = GATEWAY_ATTRIBUTES_TOPIC
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC, qos=1)[1])] = GATEWAY_ATTRIBUTES_RESPONSE_TOPIC
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_RPC_TOPIC, qos=1)[1])] = GATEWAY_RPC_TOPIC
            # self._gw_subscriptions[int(self._client.subscribe(GATEWAY_RPC_RESPONSE_TOPIC)[1])] = GATEWAY_RPC_RESPONSE_TOPIC

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        subscription = self._gw_subscriptions.get(mid)
        if subscription is not None:
            if mid == 128:
                log.error("Service subscription to topic %s - failed.", subscription)
                del self._gw_subscriptions[mid]
            else:
                log.debug("Service subscription to topic %s - successfully completed.", subscription)
                del self._gw_subscriptions[mid]

    def _on_unsubscribe(self, *args):
        log.debug(args)

    def get_subscriptions_in_progress(self):
        return True if self._gw_subscriptions else False

    def _on_message(self, client, userdata, message):
        content = self._decode(message)
        super()._on_decoded_message(self, content, message)
        self._on_decoded_message(self, content, message)

    def _on_decoded_message(self, client, content, message):
        if message.topic.startswith(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC):
            with self._lock:
                req_id = content["id"]
                # pop callback and use it
                if self._attr_request_dict[req_id]:
                    self._attr_request_dict.pop(req_id)(self, content, None)
                else:
                    log.error("Unable to find callback to process attributes response from TB")
        elif message.topic == GATEWAY_ATTRIBUTES_TOPIC:
            with self._lock:
                # callbacks for everything
                if self.__sub_dict.get("*|*"):
                    for callback in self.__sub_dict["*|*"]:
                        self.__sub_dict["*|*"][callback](self, content["data"])
                # callbacks for device. in this case callback executes for all attributes in message
                target = content["device"] + "|*"
                if self.__sub_dict.get(target):
                    for callback in self.__sub_dict[target]:
                        self.__sub_dict[target][callback](self, content["data"])
                # callback for atr. in this case callback executes for all attributes in message
                targets = [content["device"] + "|" + callback for callback in content["data"]]
                for target in targets:
                    if self.__sub_dict.get(target):
                        for sub_id in self.__sub_dict[target]:
                            self.__sub_dict[target][sub_id](self, content["data"])
        elif message.topic == GATEWAY_RPC_TOPIC:
            if self.devices_server_side_rpc_request_handler:
                self.devices_server_side_rpc_request_handler(self, content)

    def __request_attributes(self, client, device, keys, callback, type_is_client=False):
        if not keys:
            log.error("There are no keys to request")
            return False
        keys_str = ""
        for key in keys:
            keys_str += key + ","
        keys_str = keys_str[:len(keys_str) - 1]
        ts_in_millis = int(round(time.time() * 1000))
        attr_request_number = self._add_attr_request_callback(callback)
        msg = {"key": keys_str,
               "device": device,
               "client": type_is_client,
               "id": attr_request_number}
        info = self._client.publish(GATEWAY_ATTRIBUTES_REQUEST_TOPIC, dumps(msg), 1)
        self._add_timeout(attr_request_number, ts_in_millis + 30000)
        return info

    def connect(self, callback=None, min_reconnect_delay=1, timeout=120, tls=False, ca_certs=None, cert_file=None, key_file=None, keepalive=120):
        super(TBGatewayMqttClient, self).connect(callback, min_reconnect_delay, timeout, tls, ca_certs, cert_file, key_file, keepalive)
        while self.get_subscriptions_in_progress() and not self.stopped and self.is_connected():
            time.sleep(.1)

    def gw_request_shared_attributes(self, device_name, keys, callback):
        return self.__request_attributes(self, device_name, keys, callback, False)

    def gw_request_client_attributes(self, device_name, keys, callback):
        return self.__request_attributes(self, device_name, keys, callback, True)

    def gw_send_attributes(self, device, attributes, quality_of_service=1):
        self.validate(KV_VALIDATOR, attributes)
        return self.publish_data({device: attributes}, GATEWAY_MAIN_TOPIC + "attributes", quality_of_service)

    def gw_send_telemetry(self, device, telemetry, quality_of_service=1):
        if type(telemetry) is not list:
            telemetry = [telemetry]
        self.validate(DEVICE_TS_KV_VALIDATOR, telemetry)
        return self.publish_data({device: telemetry}, GATEWAY_MAIN_TOPIC + "telemetry", quality_of_service, )

    def gw_connect_device(self, device_name, device_type="default"):
        info = self._client.publish(topic=GATEWAY_MAIN_TOPIC + "connect", payload=dumps({"device": device_name, "type": device_type}), qos=self.quality_of_service)
        self.__connected_devices.add(device_name)
        log.debug("Connected device %s", device_name)
        return info

    def gw_disconnect_device(self, device_name):
        info = self._client.publish(topic=GATEWAY_MAIN_TOPIC + "disconnect", payload=dumps({"device": device_name}),
                                    qos=self.quality_of_service)
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
                self.__sub_dict.update({key: {self.__max_sub_id: callback}})
            else:
                self.__sub_dict[key].update({self.__max_sub_id: callback})
            log.info("Subscribed to %s with id %i", key, self.__max_sub_id)
            return self.__max_sub_id

    def gw_unsubscribe(self, subscription_id):
        with self._lock:
            for attribute in self.__sub_dict:
                if self.__sub_dict[attribute].get(subscription_id):
                    del self.__sub_dict[attribute][subscription_id]
                    log.info("Unsubscribed from %s, subscription id %i", attribute, subscription_id)
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
        info = self._client.publish(GATEWAY_RPC_TOPIC,
                                    dumps({"device": device, "id": req_id, "data": resp}),
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
        info = self._client.publish(GATEWAY_CLAIMING_TOPIC, dumps(claiming_request), qos=self.quality_of_service)
        return info
