import paho.mqtt.client as paho
import logging
import time
import threading
from json import loads, dumps
from jsonschema import Draft7Validator
from tb_device_mqtt import TBClient

TOPIC = "v1/gateway/"



log = logging.getLogger(__name__)

class TBGateway(TBClient):
    def __init__(self, host, token, timeout=10):
        self.__host = host
        self.__client = paho.Client()
        self.__client.username_pw_set(token)
        self.__is_connected = False
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
        self.__client.on_connect = on_connect
        self.__client.on_log = on_log

        self.__client.connect(self.__host)
        self.__client.loop_start()
        t = time.time()
        while self.__is_connected is not True:
            time.sleep(0.1)
            if time.time()-t > timeout:
                #todo what we should do if broker does not respond for timeout period?
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


# todo if one of telemetry is not valid, do we send other? prob yes
    def send_telemetry(self, telemetry, quality_of_service=1, blocking=False):
        # todo implement validation
        # todo check if entire log with Exception in logs
        # for device in telemetry.keys:
        #     try:
        #         TS_KV_VALIDATOR.validate(telemetry[device])
        #     except Exception:
        #         log.error("Invalid telemetry for device "+device)
        #
        self.__publish_data(telemetry, TOPIC+"telemetry", quality_of_service, blocking )


    def connect_device(self, device, blocking=False):
        info = self.__client.publish(topic=TOPIC+"connect", payload=dumps({"device":str(device)}), qos=1)
        if blocking:
            info.wait_for_publish()

    def disconnect_device(self, device, blocking=False):
        info = self.__client.publish(topic=TOPIC+"disconnect", payload=dumps({"device":str(device)}), qos=1)
        if blocking:
            info.wait_for_publish()