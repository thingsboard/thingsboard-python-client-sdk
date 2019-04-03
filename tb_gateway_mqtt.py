import paho.mqtt.client as paho
import logging
import time
from json import loads, dumps
from tb_device_mqtt import TBClient, DEVICE_TS_KV_VALIDATOR, KV_VALIDATOR, TBTimeoutException
from jsonschema import ValidationError
import ssl
from threading import Lock, Thread
import queue


GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes"
GATEWAY_ATTRIBUTES_REQUEST_TOPIC = "v1/gateway/attributes/request"
GATEWAY_ATTRIBUTES_RESPONSE_TOPIC = "v1/gateway/attributes/response"
TOPIC = "v1/gateway/"
RPC_TOPIC = "v1/gateway/rpc"

log = logging.getLogger(__name__)


class TBGateway(TBClient):
    def __init__(self, host, token=None):
        self.client = paho.Client()
        if token is None:
            log.warning("token is not set, connection without tls wont be established")
        else:
            self.client.username_pw_set(token)
        self.__host = host
        self.__rpc_set = False
        self.__is_connected = False
        self.__atr_request_number = 1
        self.__atr_request_dict = {}
        self.__is_attribute_requested = False
        self.__max_sub_id = 0
        self.__sub_dict = {}
        self.__connected_devices = set("*")
        self.__subscribed_to_attributes = False
        self.lock = Lock()
        self.timeout_queue = queue.Queue()
        self.timeout_thread = Thread(target=self.__timeout_check)
        self.timeout_thread.start()

        def on_connect(client, userdata, flags, rc, *extra_params):
            result_codes = {
                1: "incorrect protocol version",
                2: "invalid client identifier",
                3: "server unavailable",
                4: "bad username or password",
                5: "not authorised",
            }
            if rc == 0:
                self.__is_connected = True
                log.info("connection SUCCESS")
                if self.__rpc_set:
                    self.client.subscribe(RPC_TOPIC + "/+")
            else:
                if rc in result_codes:
                    log.error("connection FAIL with error '%i':'%s'" % (rc, result_codes[rc]))
                else:
                    log.error("connection FAIL with unknown error")

        def on_log(client, userdata, level, buf):
            log.debug(buf)

        def on_message(client, userdata, message):
            content = loads(message.payload.decode("utf-8"))
            log.info(content)
            log.info(message.topic)
            if message.topic.startswith(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC):
                with self.lock:
                    req_id = content["id"]
                    # pop callback and use it
                    if self.__atr_request_dict[req_id]:
                        self.__atr_request_dict.pop(req_id)(content, None)
                    else:
                        log.error("Unable to find callback to process attributes response from TB")
            elif message.topic == GATEWAY_ATTRIBUTES_TOPIC:
                with self.lock:
                    # callbacks for everythings
                    if self.__sub_dict.get("*|*"):
                        for x in self.__sub_dict["*|*"]:
                            self.__sub_dict["*|*"][x](content["data"])
                    # callbacks for device. in this case callback executes for all attributes in message
                    target = content["device"] + "|*"
                    if self.__sub_dict.get(target):
                        for x in self.__sub_dict[target]:
                            self.__sub_dict[target][x](content["data"])
                    # callback for atr. in this case callback executes for all attributes in message
                    targets = [content["device"] + "|" + x for x in content["data"]]
                    for target in targets:
                        if self.__sub_dict.get(target):
                            for sub_id in self.__sub_dict[target]:
                                self.__sub_dict[target][sub_id](content["data"])
            elif message.topic == RPC_TOPIC:
                if self.__on_server_side_rpc_response:
                    self.__on_server_side_rpc_response(content)

        self.client.on_connect = on_connect
        self.client.on_log = on_log
        self.client.on_message = on_message

    def connect(self, callback=None, timeout=10, tls=False, port=1883, ca_certs=None, cert_file=None, key_file=None):
        if tls:
            port = 8883
            self.client.tls_set(ca_certs=ca_certs,
                                certfile=cert_file,
                                keyfile=key_file,
                                cert_reqs=ssl.CERT_REQUIRED,
                                tls_version=ssl.PROTOCOL_TLSv1,
                                ciphers=None)
            self.client.tls_insecure_set(False)
        self.client.connect(self.__host)
        self.client.loop_start()
        t = time.time()
        while self.__is_connected is not True:
            time.sleep(0.1)
            self.__connected_devices = set("*")
            if time.time()-t > timeout:
                return False
            return True

    def __request_attributes(self, device, keys, callback, type_is_client=False):
        if not keys:
            log.error("There are no keys to request")
            return False
        if not self.__is_attribute_requested:
            self.__is_attribute_requested = True
            self.client.subscribe(GATEWAY_ATTRIBUTES_TOPIC, 1)
        tmp = ""
        for key in keys:
            tmp += key + ","
        tmp = tmp[:len(tmp) - 1]
        msg = {"key": tmp,
               "device": device,
               "client": type_is_client,
               "id": self.__atr_request_number}
        ts_in_millis = int(round(time.time() * 1000))
        with self.lock:
            self.__atr_request_dict.update({self.__atr_request_number: callback})
            self.__atr_request_number += 1
            attr_request_number = self.__atr_request_number
        self.client.publish(GATEWAY_ATTRIBUTES_REQUEST_TOPIC, dumps(msg), 1)
        self.timeout_queue.put({"ts": ts_in_millis + 30000, "attribute_request_id": attr_request_number})

    def request_shared_attributes(self, device_name, keys, callback):
        self.__request_attributes(device_name, keys, callback, False)

    def request_client_attributes(self, device_name, keys, callback):
        self.__request_attributes(device_name, keys, callback, True)

    def send_attributes(self, device, attributes, quality_of_service=1):
        try:
            KV_VALIDATOR.validate(attributes)
        except ValidationError as e:
            log.error(e)
            return False
        self.publish_data({device: attributes}, TOPIC+"attributes", quality_of_service)

    def send_telemetry(self, device, telemetry, quality_of_service=1):
        if type(telemetry) is not list:
            telemetry = [telemetry]
        try:
            DEVICE_TS_KV_VALIDATOR.validate(telemetry)
        except ValidationError as e:
            log.error(e)
            return False
        self.publish_data({device: telemetry}, TOPIC+"telemetry", quality_of_service,)

    def connect_device(self, device_name, wait_for_publish=False):
        info = self.client.publish(topic=TOPIC + "connect", payload=dumps({"device": device_name}), qos=1)
        if wait_for_publish:
            info.wait_for_publish()
        self.__connected_devices.add(device_name)
        log.debug("Connected device {name}".format(name=device_name))

    def unsubscribe(self, subscription_id):
        with self.lock:
            for x in self.__sub_dict:
                if self.__sub_dict[x].get(subscription_id):
                    del self.__sub_dict[x][subscription_id]
                    log.debug("Unsubscribed from {attribute}, subscription id {sub_id}".format(attribute=x,
                                                                                               sub_id=subscription_id))

    def disconnect_device(self, device_name, wait_for_publish=False):
        info = self.client.publish(topic=TOPIC + "disconnect", payload=dumps({"device": device_name}), qos=1)
        if wait_for_publish:
            info.wait_for_publish()
        self.__connected_devices.remove(device_name)
        log.debug("Disconnected device {name}".format(name=device_name))

    def subscribe_to_all(self, callback):
        return self.subscribe_to_attribute("*", "*", callback)

    def subscribe_to_attributes(self, device, callback):
        return self.subscribe_to_attribute(device, "*", callback)

    def subscribe_to_attribute(self, device, attribute, callback):
        if device not in self.__connected_devices:
            log.error("Device {name} not connected".format(name=device))
            return False
        if not self.__subscribed_to_attributes:
            self.client.subscribe(GATEWAY_ATTRIBUTES_TOPIC, qos=1)
            self.__subscribed_to_attributes = True
        with self.lock:
            self.__max_sub_id += 1
            key = device+"|"+attribute
            if key not in self.__sub_dict:
                self.__sub_dict.update({key: {self.__max_sub_id: callback}})
            else:
                self.__sub_dict[key].update({self.__max_sub_id: callback})
            log.debug("Subscribed to {key} with id {id}".format(key=key, id=self.__max_sub_id))
            return self.__max_sub_id

    def set_server_side_rpc_request_handler(self, handler):
        self.__rpc_set = True
        self.client.subscribe(RPC_TOPIC + "/+")
        self.__on_server_side_rpc_response = handler

    def respond(self, device, req_id, resp, quality_of_service=1, wait_for_publish=False):
        if quality_of_service != 0 and quality_of_service != 1:
            log.error("Quality of service (qos) value must be 0 or 1")
            return
        info = self.client.publish(RPC_TOPIC,
                                   dumps({"device": device, "id": req_id, "data": resp}),
                                   qos=quality_of_service)
        if wait_for_publish:
            info.wait_for_publish()

    def __timeout_check(self):
        while True:
            try:
                item = self.timeout_queue.get()
                if item is not None:
                    while True:
                        current_ts_in_millis = int(round(time.time() * 1000))
                        if current_ts_in_millis > item["ts"]:
                            break
                        else:
                            time.sleep(0.1)
                    callback = None
                    if item.get("attribute_request_id"):
                        callback = self.__atr_request_dict.pop(item["attribute_request_id"])
                    if callback is not None:
                        callback(None, TBTimeoutException("Timeout while waiting for reply from ThingsBoard!"))
                else:
                    time.sleep(0.1)
            except Exception as e:
                log.warning(e)
