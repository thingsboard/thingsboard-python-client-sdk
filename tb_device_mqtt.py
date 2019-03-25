import paho.mqtt.client as paho
import logging
import time
import threading
from json import loads, dumps
from jsonschema import Draft7Validator
import ssl

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

RPC_RESPONSE_TOPIC = 'v1/devices/me/rpc/response/'
RPC_REQUEST_TOPIC = 'v1/devices/me/rpc/request/'
ATTRIBUTES_TOPIC = 'v1/devices/me/attributes'
ATTRIBUTES_TOPIC_REQUEST = 'v1/devices/me/attributes/request/'
ATTRIBUTES_TOPIC_RESPONSE = 'v1/devices/me/attributes/response/'
TELEMETRY_TOPIC = 'v1/devices/me/telemetry'

log = logging.getLogger(__name__)


class TBClient:
    def __init__(self, host, token):
        self.client = paho.Client()
        self.__host = host
        self.client.username_pw_set(token)
        self.__is_attribute_requested = False
        self.__is_connected = False
        self.client.on_connect = None
        self.client.on_log = None
        self.client.on_publish = None
        self.client.on_message = None
        self.__on_server_side_rpc_response = None
        self.__on_client_side_rpc_response = None
        self.__connect_callback = None
        self.__rpc_set = None
        #todo serialize sequence and sub_dict to run normally if re-load script
        self.__atr_request_number = 1
        self.__max_sub_id = 1
        self.__sub_dict = {}
        self.__client_rpc_dict = {}
        self.__atr_request_dict = {}

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
                    self.client.subscribe(RPC_REQUEST_TOPIC + '+')
                log.info("connection SUCCESS")
            else:
                if rc in result_codes:
                    log.error("connection FAIL with error {rc} {explanation}".format(rc=rc,
                                                                                     explanation=result_codes[rc]))
                else:
                    log.error("connection FAIL with unknown error")

        def on_publish(client, userdata, result):
            log.debug("Data published to ThingsBoard!")
            pass

        def on_message(client, userdata, message):
            content = loads(message.payload.decode("utf-8"))
            log.info(content)
            log.info(message.topic)
            if message.topic.startswith(RPC_REQUEST_TOPIC):
                request_id = message.topic[len(RPC_REQUEST_TOPIC):len(message.topic)]
                self.__on_server_side_rpc_response(request_id, content)
            # todo fix error payload in wrong format?
            elif message.topic.startswith(RPC_RESPONSE_TOPIC):
                request_id = message.topic[len(RPC_RESPONSE_TOPIC):len(message.topic)]
                if self.__client_rpc_dict.get(request_id):
                    x = self.__client_rpc_dict.pop(request_id)
                    x(request_id, content)
                self.__on_client_side_rpc_response(request_id, content)
            elif message.topic == ATTRIBUTES_TOPIC:
                # callbacks for everything
                if self.__sub_dict.get("*"):
                    for x in self.__sub_dict["*"]:
                        self.__sub_dict["*"][x](content)

                # specific callback
                keys = content.keys()
                keys_list = []
                for key in keys:
                    keys_list.append(key)
                # iterate through message
                for key in keys_list:
                    # find key in our dict
                    if self.__sub_dict.get(key):
                        for x in self.__sub_dict[key]:
                            self.__sub_dict[key][x](content)
            elif message.topic.startswith(ATTRIBUTES_TOPIC_RESPONSE):
                req_id = int(message.topic[len(ATTRIBUTES_TOPIC+"/response/"):])
                # pop callback and use it
                if self.__atr_request_dict[req_id]:
                    self.__atr_request_dict.pop(req_id)(content)
                else:
                    log.error("Unable to find callback to process attributes response from TB")

        self.client.on_connect = on_connect
        self.client.on_log = on_log
        self.client.on_publish = on_publish
        self.client.on_message = on_message

    def respond(self, req_id, resp, quality_of_service=1, blocking=False):
        if quality_of_service != 0 and quality_of_service != 1:
            log.exception("Quality of service (qos) value must be 0 or 1")
            return
        info = self.client.publish(RPC_RESPONSE_TOPIC + req_id, resp, qos=quality_of_service)
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

        self.__client_rpc_dict.update({find_max_rpc_id(): callback})
        payload = {"method": method, "params": params}
        self.client.publish(RPC_REQUEST_TOPIC + str(find_max_rpc_id()),
                            dumps(payload),
                            qos=1)

    def set_client_side_rpc_request_handler(self, handler):
        self.__rpc_set = True
        if self.__is_connected:
            self.client.subscribe(RPC_RESPONSE_TOPIC + '+')
        self.__on_client_side_rpc_response = handler

    def set_server_side_rpc_request_handler(self, handler):
        self.__rpc_set = True
        if self.__is_connected:
            self.client.subscribe(RPC_REQUEST_TOPIC + '+')
        self.__on_server_side_rpc_response = handler

    def connect(self, callback=None, timeout=10, tls=False, port=1883, ca_certs=None, cert_file=None):
        if tls:
            port = 8883
            self.client.tls_set(ca_certs=ca_certs,
                                certfile=cert_file,
                                keyfile=None,
                                cert_reqs=ssl.CERT_REQUIRED,
                                tls_version=ssl.PROTOCOL_TLSv1,
                                ciphers=None)
            self.client.tls_insecure_set(False)
        self.client.connect(self.__host, port)
        self.client.loop_start()
        self.__connect_callback = callback
        t = time.time()

        while self.__is_connected is not True:
            time.sleep(0.5)
            if time.time()-t > timeout:
                return False
        return True

    def disconnect(self):
        self.client.disconnect()
        log.info("Disconnected from ThingsBoard!")

    def publish_data(self, data, topic, qos, blocking):
        def send_d():
            info = self.client.publish(topic, data, qos)
            info.wait_for_publish()
        data = dumps(data)
        if qos != 0 and qos != 1:
            log.exception("Quality of service (qos) value must be 0 or 1")
            return
        if blocking:
            t = threading.Thread(target=send_d)
            t.start()
        else:
            self.client.publish(topic, data, qos)

    def send_telemetry(self, telemetry, quality_of_service=1, blocking=False):
        if telemetry.get("ts"):
            TS_KV_VALIDATOR.validate(telemetry)
        else:
            KV_VALIDATOR.validate(telemetry)
        self.publish_data(telemetry, TELEMETRY_TOPIC, quality_of_service, blocking)

    def send_attributes(self, attributes, quality_of_service=1, blocking=False):
        KV_VALIDATOR.validate(attributes)
        self.publish_data(attributes, ATTRIBUTES_TOPIC, quality_of_service, blocking)

    def unsubscribe(self, subscription_id):
        #todo check if it works
        empty_keys = []
        for attribute in self.__sub_dict.keys():
            if self.__sub_dict[attribute].get(subscription_id):
                del self.__sub_dict[attribute][subscription_id]
                log.debug("Unsubscribed from {attribute}, subscription id {sub_id}".format(attribute=attribute,
                                                                                               sub_id=subscription_id))
            if not self.__sub_dict[attribute]:
                empty_keys.append(attribute)

        for key in empty_keys:
            del self.__sub_dict[key]

    def subscribe(self, key="*", callback=None, quality_of_service=1):
        if quality_of_service != 0 and quality_of_service != 1:
            log.error("qos must be 0 or 1, got {qos}".format(qos=quality_of_service))
            return False
        self.client.subscribe(ATTRIBUTES_TOPIC, qos=quality_of_service)
        self.__max_sub_id += 1
        if key not in self.__sub_dict:
            self.__sub_dict.update({key: {self.__max_sub_id: callback}})
        else:
            self.__sub_dict[key].update({self.__max_sub_id: callback})
        log.debug("Subscribed to {key} with id {id}".format(key=key, id=self.__max_sub_id))
        return self.__max_sub_id

    def request_attributes(self, client_keys=None, shared_keys=None, callback=None):
        if client_keys is None and shared_keys is None:
            log.error("There are no keys to request")
            return False
        if not self.__is_attribute_requested:
            self.__is_attribute_requested = True
            self.client.subscribe(ATTRIBUTES_TOPIC + "/response/+", 1)
        msg = {}
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
        self.client.publish(topic=ATTRIBUTES_TOPIC_REQUEST + str(self.__atr_request_number),
                            payload=dumps(msg),
                            qos=1)
        self.__atr_request_dict.update({self.__atr_request_number: callback})
        self.__atr_request_number += 1
