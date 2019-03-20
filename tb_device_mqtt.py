import paho.mqtt.client as paho
import logging
import time
import threading
from json import loads, dumps
from jsonschema import Draft7Validator

KV_SCHEMA = {
    "type": "object",
    "patternProperties":
        {
            ".": {"type": ["integer",
                           "string",
                           "boolean",
                           "number"]}
        },
    "minProperties": 1,
}

TS_KV_SCHEMA = {
    "type": "object",
    "properties": {
        "ts": {
            "type": "integer"
        },
        "values": KV_SCHEMA
    },
    "additionalProperties": False
}

KV_VALIDATOR = Draft7Validator(KV_SCHEMA)
TS_KV_VALIDATOR = Draft7Validator(TS_KV_SCHEMA)

TB_RPC_RESPONSE_TOPIC_PREFIX = 'v1/devices/me/rpc/response/'
TB_RPC_REQUEST_TOPIC_PREFIX = 'v1/devices/me/rpc/request/'
ATTRIBUTES_TOPIC = 'v1/devices/me/attributes'
ATTRIBUTES_TOPIC_REQUEST = 'v1/devices/me/attributes/request/'
ATTRIBUTES_TOPIC_RESPONSE = 'v1/devices/me/attributes/response/'
TELEMETRY_TOPIC = 'v1/devices/me/telemetry'

log = logging.getLogger(__name__)


class TBClient:
    class __SubscriptionInfo:
        def __init__(self, sub_id, cb):
            self.subscription_id = sub_id
            self.callback = cb

    def __init__(self, host, token):
        self.__client = paho.Client()
        self.__host = host
        self.__client.username_pw_set(token)
        self.__is_attribute_requested = False
        self.__is_connected = False
        self.__sub_dict = {}
        self.__client_rpc_dict = {}
        self.__atr_request_dict = {}
        self.__client.on_disconnect = None
        self.__client.on_connect = None
        self.__client.on_log = None
        self.__client.on_publish = None
        self.__client.on_message = None
        self.__on_server_side_rpc_response = None
        self.__on_client_side_rpc_response = None
        self.__connect_callback = None
        self.__rpc_set = None
        self.__rpc_request_number = 1


        def on_log(client, userdata, level, buf):
            log.debug(buf)
            pass

        def on_connect(client, userdata, flags, rc, *extra_params):
            result_codes = {
                1: "incorrect protocol version",
                2: "invalid client identifier",
                3: "server unavailable",
                4: "bad username or password",
                5: "not authorised",
            }
            if self.__connect_callback:
                self.__connect_callback(client, userdata, flags, rc, *extra_params)
            if rc == 0:
                self.__is_connected = True
                if self.__rpc_set:
                    self.__client.subscribe(TB_RPC_REQUEST_TOPIC_PREFIX + '+')
                log.info("connection SUCCESS")
            else:
                if rc in result_codes:
                    log.error("connection FAIL with error '%i':'%s'" % (rc, result_codes[rc]))
                else:
                    log.error("connection FAIL with unknown error")

        def on_disconnect(client, userdata, rc):
            self.__is_connected = False
            if self.__disconnect_callback:
                self.__disconnect_callback(userdata, rc)
            if rc == 0:
                log.info("disconnect SUCCESS")
            else:
                log.error("disconnect FAIL with error code %i" % rc)

        def on_publish(client, userdata, result):
            log.debug("Data published to ThingsBoard!")
            pass

        def on_message(client, userdata, message):
            content = loads(message.payload.decode("utf-8"))
            log.info(content)
            log.info(message.topic)
            if message.topic.startswith(TB_RPC_REQUEST_TOPIC_PREFIX):
                request_id = message.topic[len(TB_RPC_REQUEST_TOPIC_PREFIX):len(message.topic)]
                self.__on_server_side_rpc_response(request_id, content)
            # todo fix error payload in wrong format?
            elif message.topic.startswith(TB_RPC_RESPONSE_TOPIC_PREFIX):
                request_id = message.topic[len(TB_RPC_RESPONSE_TOPIC_PREFIX):len(message.topic)]
                if self.__client_rpc_dict.get(request_id):
                    x = self.__client_rpc_dict.pop(request_id)
                    x(request_id, content)
                self.__on_client_side_rpc_response(request_id, content)
            elif message.topic == ATTRIBUTES_TOPIC:
                message = eval(content)
                for key in self.__sub_dict.keys():
                    if self.__sub_dict.get(key):
                        for item in self.__sub_dict.get(key):
                            item.callback(message)
            elif message.topic.startswith(ATTRIBUTES_TOPIC_RESPONSE):
                req_id = int(message.topic[len(ATTRIBUTES_TOPIC+"/response/"):])
                # pop callback and use it
                if self.__atr_request_dict[req_id]:
                    self.__atr_request_dict.pop(req_id)(content)
                else:
                    log.error("Unable to find callback to process attributes response from TB")

        self.__client.on_disconnect = on_disconnect
        self.__client.on_connect = on_connect
        self.__client.on_log = on_log
        self.__client.on_publish = on_publish
        self.__client.on_message = on_message

    def respond(self, req_id, resp, quality_of_service=1, blocking=False):
        if quality_of_service != 0 and quality_of_service != 1:
            log.exception("Quality of service (qos) value must be 0 or 1")
            return
        info = self.__client.publish(TB_RPC_RESPONSE_TOPIC_PREFIX + req_id, resp, qos=quality_of_service)
        if blocking:
            info.wait_for_publish()

    def send_rpc_call(self, method, params, callback):
        #todo validate parameters?
        #todo create third dict for ids and callbacks and process it?
        #todo replace with labmda
        def find_max_rpc_id():
            res = 1
            for item in self.__client_rpc_dict:
                if item > res:
                    res = item
            return res

        self.__client_rpc_dict.update({find_max_rpc_id():callback})
        payload = {"method": method, "params": params}
        self.__client.publish(TB_RPC_REQUEST_TOPIC_PREFIX + str(find_max_rpc_id()),
                              dumps(payload),
                              qos=1)

    def set_client_side_rpc_request_handler(self, handler):
        self.__rpc_set = True
        if self.__is_connected:
            self.__client.subscribe(TB_RPC_RESPONSE_TOPIC_PREFIX + '+')
        self.__on_client_side_rpc_response = handler

    def set_server_side_rpc_request_handler(self, handler):
        self.__rpc_set = True
        if self.__is_connected:
            self.__client.subscribe(TB_RPC_REQUEST_TOPIC_PREFIX + '+')
        self.__on_server_side_rpc_response = handler

    def __connect_callback(self, *args):
        pass

    def connect(self, callback=None, timeout=10):
        self.__client.connect(self.__host)
        self.__client.loop_start()
        self.__connect_callback = callback
        t = time.time()

        while self.__is_connected is not True:
            time.sleep(0.5)
            if time.time()-t > timeout:
                return False
        return True

    def disconnect(self):
        self.__client.disconnect()
        log.info("Disconnected from ThingsBoard!")

    def __disconnect_callback(self, *args):
        pass

    def __publish_data(self, data, topic, qos, blocking):
        def send_d():
            info = self.__client.publish(topic, data, qos)
            info.wait_for_publish()
        data = dumps(data)
        if qos != 0 and qos != 1:
            log.exception("Quality of service (qos) value must be 0 or 1")
            return
        if blocking:
            t = threading.Thread(target=send_d)
            t.start()
        else:
            self.__client.publish(topic, data, qos)

    def send_telemetry(self, telemetry, quality_of_service=1, blocking=False):
        if telemetry.get("ts"):
            TS_KV_VALIDATOR.validate(telemetry)
        else:
            KV_VALIDATOR.validate(telemetry)
        self.__publish_data(telemetry, TELEMETRY_TOPIC, quality_of_service, blocking)

    def send_attributes(self, attributes, quality_of_service=1, blocking=False):
        KV_VALIDATOR.validate(attributes)
        self.__publish_data(attributes, ATTRIBUTES_TOPIC, quality_of_service, blocking)

    def unsubscribe(self, subscription_id):
        empty_keys = []
        for attribute in self.__sub_dict.keys():
            for x in self.__sub_dict[attribute]:
                if x.subscription_id == subscription_id:
                    self.__sub_dict[attribute].remove(x)
                    log.debug("Unsubscribed from " + attribute + ". subscription id " + str(subscription_id))
            if not self.__sub_dict[attribute]:
                empty_keys.append(attribute)

        for key in empty_keys:
            del self.__sub_dict[key]

    def subscribe(self, key="*", callback=None, quality_of_service=1):
        self.__client.subscribe(ATTRIBUTES_TOPIC, qos=quality_of_service)

        def find_max_sub_id():
            res = 1
            for attrib in self.__sub_dict.keys():
                for item in self.__sub_dict[attrib]:
                    if item.subscription_id > res:
                        res = item.subscription_id
            return res

        inst = self.__SubscriptionInfo(find_max_sub_id(), callback)
        # subscribe to everything
        if key == "*":
            for attr in self.__sub_dict.keys():
                if inst not in self.__sub_dict[attr]:
                    self.__sub_dict[attr].append(inst)
                    log.debug("Subscribed to " + attr + ", subscription id " + str(inst.subscription_id))
        # if attribute doesn't exist create it with subscription
        elif key not in self.__sub_dict.keys():
            self.__sub_dict.update({key: [inst]})
            log.debug("Subscribed to " + key + ", subscription id " + str(inst.subscription_id))
        # if attribute exists create subscription
        else:
            self.__sub_dict[key].append(inst)
            log.debug("Subscribed to " + key + ", subscription id " + str(inst.subscription_id))
        return inst.subscription_id

    def request_attributes(self, client_keys=None, shared_keys=None, callback=None):
        if not self.__is_attribute_requested:
            self.__is_attribute_requested = True
            self.__client.subscribe(ATTRIBUTES_TOPIC+"/response/+", 1)
        msg = {}
        if client_keys is None and shared_keys is None:
            log.error("There are no keys to request")
            return False
        if client_keys:
            tmp = ""
            for key in client_keys:
                tmp += key + ","
            tmp = tmp[:len(tmp) - 1]
            msg.update({"clientKeys": tmp})
        if shared_keys:
            tmp = ""
            for key in shared_keys:
                tmp += key + ","
            tmp = tmp[:len(tmp) - 1]
            msg.update({"sharedKeys": tmp})
        self.__client.publish(topic=ATTRIBUTES_TOPIC_REQUEST + str(self.__rpc_request_number),
                              payload=dumps(msg),
                              qos=1)
        self.__atr_request_dict.update({self.__rpc_request_number: callback})
        self.__rpc_request_number += 1
