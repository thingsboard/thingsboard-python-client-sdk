import paho.mqtt.client as paho
import logging
import time
from json import loads, dumps
from jsonschema import Draft7Validator
import ssl
from jsonschema import ValidationError
from threading import Lock

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
SCHEMA_FOR_CLIENT_RPC = {
    "type": "object",
    "patternProperties":
        {
            ".": {"type": ["integer",
                           "string",
                           "boolean",
                           "number"]}
        },
    "minProperties": 0,
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
DEVICE_TS_KV_SCHEMA = {
    "type": "array",
    "items": TS_KV_SCHEMA
}
DEVICE_TS_OR_KV_SCHEMA = {
    "type": "array",
    "items":    {
        "anyOf":
            [
                TS_KV_SCHEMA,
                KV_SCHEMA
            ]
    }
}
RPC_VALIDATOR = Draft7Validator(SCHEMA_FOR_CLIENT_RPC)
KV_VALIDATOR = Draft7Validator(KV_SCHEMA)
TS_KV_VALIDATOR = Draft7Validator(TS_KV_SCHEMA)
DEVICE_TS_KV_VALIDATOR = Draft7Validator(DEVICE_TS_KV_SCHEMA)
DEVICE_TS_OR_KV_VALIDATOR = Draft7Validator(DEVICE_TS_OR_KV_SCHEMA)

RPC_RESPONSE_TOPIC = 'v1/devices/me/rpc/response/'
RPC_REQUEST_TOPIC = 'v1/devices/me/rpc/request/'
ATTRIBUTES_TOPIC = 'v1/devices/me/attributes'
ATTRIBUTES_TOPIC_REQUEST = 'v1/devices/me/attributes/request/'
ATTRIBUTES_TOPIC_RESPONSE = 'v1/devices/me/attributes/response/'
TELEMETRY_TOPIC = 'v1/devices/me/telemetry'
log = logging.getLogger(__name__)


class TBClient:
    def __init__(self, host, token=None):
        self.client = paho.Client()
        self.__host = host
        if token == "":
            log.warning("token is not set, connection without tls wont be established")
        else:
            self.client.username_pw_set(token)
        self.lock = Lock()
        self.__is_subscribed_to_attributes = False
        self.__is_attribute_requested = False
        self.__is_subscribed_to_server_responses = False
        self.__is_connected = False
        self.client.on_connect = None
        self.client.on_log = None
        self.client.on_publish = None
        self.client.on_message = None
        self.__on_server_side_rpc_response = None
        self.__on_client_side_rpc_response = None
        self.__connect_callback = None
        self.__rpc_set = None
        self.__atr_request_number = 1
        self.__max_sub_id = 1
        self.__client_rpc_number = 1
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
                log.info("connection SUCCESS")
                if self.__rpc_set:
                    self.client.subscribe(RPC_REQUEST_TOPIC + '+')
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
                if self.__on_server_side_rpc_response:
                    self.__on_server_side_rpc_response(request_id, content)
            elif message.topic.startswith(RPC_RESPONSE_TOPIC):
                request_id = int(message.topic[len(RPC_RESPONSE_TOPIC):len(message.topic)])
                self.__client_rpc_dict.pop(request_id)(request_id, content)
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
                self.__atr_request_dict.pop(req_id)(content)

        self.client.on_connect = on_connect
        self.client.on_log = on_log
        self.client.on_publish = on_publish
        self.client.on_message = on_message

    def respond(self, req_id, resp, quality_of_service=1, wait_for_publish=False):
        if quality_of_service != 0 and quality_of_service != 1:
            log.error("Quality of service (qos) value must be 0 or 1")
            return
        info = self.client.publish(RPC_RESPONSE_TOPIC + req_id, resp, qos=quality_of_service)
        if wait_for_publish:
            info.wait_for_publish()

    def send_rpc_call(self, method, params, callback):
        try:
            RPC_VALIDATOR.validate(params)
        except ValidationError as e:
            log.error(e)
            return False
        if not self.__is_subscribed_to_server_responses:
            self.__is_subscribed_to_server_responses = True
            self.client.subscribe(RPC_RESPONSE_TOPIC + '+', qos=1)
        with self.lock:
            self.__client_rpc_number += 1
            self.__client_rpc_dict.update({self.__client_rpc_number: callback})
            payload = {"method": method, "params": params}
            self.client.publish(RPC_REQUEST_TOPIC + str(self.__client_rpc_number),
                                dumps(payload),
                                qos=1)

    def set_server_side_rpc_request_handler(self, handler):
        self.__rpc_set = True
        if self.__is_connected:
            self.client.subscribe(RPC_REQUEST_TOPIC + '+')
        self.__on_server_side_rpc_response = handler

    def connect(self, callback=None, timeout=10, tls=False, port=1883, ca_certs=None, cert_file=None, key_file=None):
        if tls:
            port = 8883
            self.client.tls_set(ca_certs=ca_certs,
                                certfile=cert_file,
                                keyfile=key_file,
                                cert_reqs=ssl.CERT_REQUIRED,
                                tls_version=ssl.PROTOCOL_TLSv1_2,
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

    def publish_data(self, data, topic, qos):
        data = dumps(data)
        if qos != 0 and qos != 1:
            log.exception("Quality of service (qos) value must be 0 or 1")
            return False
        else:
            self.client.publish(topic, data, qos)

    def send_telemetry(self, telemetry, quality_of_service=1):
        if type(telemetry) is not list:
            telemetry = [telemetry]
        try:
            DEVICE_TS_OR_KV_VALIDATOR.validate(telemetry)
        except ValidationError as e:
            log.error(e)
            return False
        self.publish_data(telemetry, TELEMETRY_TOPIC, quality_of_service)

    def send_attributes(self, attributes, quality_of_service=1):
        try:
            KV_VALIDATOR.validate(attributes)
        except ValidationError as e:
            log.error(e)
            return False
        self.publish_data(attributes, ATTRIBUTES_TOPIC, quality_of_service)

    def unsubscribe(self, subscription_id):
        with self.lock:
            for x in self.__sub_dict:
                if self.__sub_dict[x].get(subscription_id):
                    del self.__sub_dict[x][subscription_id]
                    log.debug("Unsubscribed from {attribute}, subscription id {sub_id}".format(attribute=x,
                                                                                               sub_id=subscription_id))
            self.__sub_dict = dict((k, v) for k, v in self.__sub_dict.items() if v is not {})

    def subscribe_to_everything(self, callback):
        self.subscribe("*", callback)

    def subscribe(self, key, callback):
        if not self.__is_subscribed_to_attributes:
            info = self.client.subscribe(ATTRIBUTES_TOPIC, qos=1)
            self.client.__is_subscribed_to_attributes = True
        with self.lock:
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
        with self.lock:
            self.__atr_request_number += 1
            self.__atr_request_dict.update({self.__atr_request_number: callback})
            self.client.publish(topic=ATTRIBUTES_TOPIC_REQUEST + str(self.__atr_request_number),
                                payload=dumps(msg),
                                qos=1)

