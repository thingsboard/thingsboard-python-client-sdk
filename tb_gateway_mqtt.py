import paho.mqtt.client as paho
import logging
import time
from json import loads, dumps
from tb_device_mqtt import TBClient, TS_KV_VALIDATOR, KV_VALIDATOR
GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes"
GATEWAY_ATTRIBUTES_REQUEST_TOPIC = "v1/gateway/attributes/request"
GATEWAY_ATTRIBUTES_RESPONSE_TOPIC = "v1/gateway/attributes/response"
TOPIC = "v1/gateway/"
log = logging.getLogger(__name__)


class TBGateway(TBClient):
    def __init__(self, host, token, timeout=10):
        self.__host = host
        self.client = paho.Client()
        self.client.username_pw_set(token)
        self.__is_connected = False
        self.__atr_request_number = 1
        self.__atr_request_dict = {}
        self.__is_attribute_requested = False
        self.__max_sub_id = 0
        self.__sub_dict = {}
        self.__connected_devices = set()

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
                req_id = content["id"]
                # pop callback and use it
                if self.__atr_request_dict[req_id]:
                    self.__atr_request_dict.pop(req_id)(content)
                else:
                    log.error("Unable to find callback to process attributes response from TB")
            elif message.topic == GATEWAY_ATTRIBUTES_TOPIC:
                print(content["device"])
                pass
        self.client.on_connect = on_connect
        self.client.on_log = on_log
        self.client.on_message = on_message

    def connect(self, callback=None, timeout=10):
        self.client.connect(self.__host)
        self.client.loop_start()
        t = time.time()
        while self.__is_connected is not True:
            time.sleep(0.1)
            self.__connected_devices = set()
            if time.time()-t > timeout:
                #todo what we should do if broker does not respond for timeout period?
                return False
            return True

    def __request_attributes(self, device, keys, type_is_client=False, callback=None):
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
        if callback:
            self.__atr_request_dict.update({self.__atr_request_number: callback})
        self.__atr_request_number += 1
        self.client.publish(GATEWAY_ATTRIBUTES_REQUEST_TOPIC, dumps(msg), 1)

    def request_shared_attributes(self, device_name, keys, callback):
        self.__request_attributes(device_name, keys, False, callback)

    def request_client_attributes(self, device_name, keys, callback):
        self.__request_attributes(device_name, keys, True, callback)

    def send_attributes(self, attributes, quality_of_service=1, blocking=False):
        for device in attributes.keys():
            for attr_in_device in attributes[device]:
                try:
                    KV_VALIDATOR.validate(attr_in_device)
                except Exception as e:
                    log.error("Invalid telemetry for device {device}\n{full_text}".format(device=device,
                                                                                          full_text=e))
        #now we send all attributes, even invalid
        self.publish_data(attributes, TOPIC+"attributes", quality_of_service, blocking)

    def send_telemetry(self, device, telemetry, quality_of_service=0, blocking=False):
        telemetry = {"ts": 1111111, values:{"lol":"troll"}}

        for device in telemetry.keys():
            for telemetry_in_device in telemetry[device]:
                try:
                    TS_KV_VALIDATOR.validate(telemetry_in_device)
                except Exception as e:
                    log.error("Invalid telemetry for device {device}\n{full_text}".format(device=device,
                                                                                          full_text=e))
        # now we send all telemetry, even invalid
        self.publish_data(telemetry, TOPIC+"telemetry", quality_of_service, blocking)

    def connect_device(self, device, blocking=False):
        info = self.client.publish(topic=TOPIC + "connect", payload=dumps({"device": str(device)}), qos=1)
        if blocking:
            info.wait_for_publish()
        self.__connected_devices.add(device)
    def disconnect_device(self, device, blocking=False):
        info = self.client.publish(topic=TOPIC + "disconnect", payload=dumps({"device": str(device)}), qos=1)
        if blocking:
            info.wait_for_publish()
        self.__connected_devices.remove(device)

    def subscribe_to_all(self, callback):
        self.subscribe_to_attribute("*", "*", callback)

    def subscribe_to_attributes(self, device, callback):
        self.subscribe_to_attribute(device, "*", callback)

    def subscribe_to_attribute(self, device, attribute, callback):
        if device not in self.__connected_devices:
            log.error("Device {name} not connected".format(name=device))
        self.client.subscribe(GATEWAY_ATTRIBUTES_TOPIC, qos=1)
        self.__max_sub_id += 1
        key = device+"|"+attribute
        if key not in self.__sub_dict:
            self.__sub_dict.update({key: {self.__max_sub_id: callback}})
        else:
            self.__sub_dict[key].update({self.__max_sub_id: callback})
        log.debug("Subscribed to {key} with id {id}".format(key=key, id=self.__max_sub_id))
        return self.__max_sub_id

    def unsubscribe(self, subscription_id):
        #todo fill
        pass
