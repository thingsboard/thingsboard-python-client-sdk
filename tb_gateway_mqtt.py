import paho.mqtt.client as paho
import logging
import time
from json import loads, dumps
from jsonschema import Draft7Validator
from tb_device_mqtt import TBClient, TS_KV_VALIDATOR, KV_VALIDATOR
GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes"
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
            pass

        def on_message(client, userdata, message):
            content = loads(message.payload.decode("utf-8"))
            log.info(content)
            log.info(message.topic)

        self.client.on_connect = on_connect
        self.client.on_log = on_log

        self.client.connect(self.__host)
        self.client.loop_start()
        t = time.time()
        while self.__is_connected is not True:
            time.sleep(0.1)
            if time.time()-t > timeout:
                #todo what we should do if broker does not respond for timeout period?
                pass

    def request_attributes(self, device, keys, type_is_client=None, callback=None):
        if not keys:
            log.error("There are no keys to request")
            return False
        if not self.__is_attribute_requested:
            self.__is_attribute_requested = True
            self.client.subscribe(GATEWAY_ATTRIBUTES_TOPIC, 1)
        msg = {}
        #todo check if order of data in dict is irrelevant


        tmp = ""
        for key in keys:
            tmp += key + ","
        tmp = tmp[:len(tmp) - 1]

        self.__atr_request_dict.update({self.__atr_request_number: callback})
        self.__atr_request_number += 1
        #todo finish it

        #Topic: v1 / gateway / attributes / request
        #Message: {"id": $request_id, "device": "Device A", "client": true, "key": "attribute1"}

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

    def send_telemetry(self, telemetry, quality_of_service=1, blocking=False):
        # validation implemented, should we use it?
        # if one telemetry is invalid, do we send other? if yes, todo pop invalid telemetry and send other
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

    def disconnect_device(self, device, blocking=False):
        info = self.client.publish(topic=TOPIC + "disconnect", payload=dumps({"device": str(device)}), qos=1)
        if blocking:
            info.wait_for_publish()
