import paho.mqtt.client as paho
import logging, time
from copy import deepcopy
from json import dumps, dump, loads
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
infoHandler = logging.FileHandler('info.log')
errorHandler = logging.FileHandler('errors.log')
infoHandler.setLevel(logging.INFO)
errorHandler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
infoHandler.setFormatter(formatter)
log.addHandler(infoHandler)
log.addHandler(errorHandler)


class TB_client:
    def __init__(self, host, token):
        self.client = paho.Client()
        self.host = host
        self.client.username_pw_set(token)
        self.callback = None

        def _on_log(client, userdata, level, buf):
            log.info(buf)

        def _on_connect(client, userdata, flags, rc, *extra_params):
            result_codes = {
                1: "incorrect protocol version",
                2: "invalid client identifier",
                3: "server unavailable",
                4: "bad username or password",
                5: "not authorised",
            }
            if rc == 0:
                log.info("connection SUCCESS")

            else:
                if rc in result_codes:
                    log.error("connection FAIL with error '%i':'%s'" % (rc, result_codes[rc]))
                else:
                    log.error("connection FAIL with unknown error")

        def _on_disconnect(client, userdata, rc):
            if rc == 0:
                log.info("disconnect SUCCESS")
            else:
                log.error("disconnect FAIL with error code %i" % rc)

        def _on_publish(client, userdata, result):
            log.info("data published")
        def _on_subscribe(client, userdata, result):

            log.info("subscribe ", result)

        def _on_message(client, userdata, message):
            log.info(message.payload.decode("utf-8"))
            log.info(message.topic)
            if message.topic == 'v1/devices/me/attributes':
                self.callback = message.payload.decode("utf-8")
                full_json = loads(self.callback)
                new_json = deepcopy(full_json)
                for i in full_json:
                    if i not in self.attributes:
                        new_json.pop(i, None)
                with open('callback.json', 'w') as outfile:
                    dump(new_json, outfile)



        self.client.on_disconnect = _on_disconnect
        self.client.on_connect = _on_connect
        self.client.on_log = _on_log
        self.client.on_publish = _on_publish
        self.client.on_message = _on_message
       # self.client.on_subscribe = _on_subscribe

    def loop(self):
        return self.client.loop()

    def connect(self):
        self.client.connect(self.host)

    def disconnect(self):
        self.client.disconnect()

    def send_telemetry(self, telemetry, send_nonstop=False, quality_of_service=0):
        if not send_nonstop:
            self.client.loop_start()
            self.client.publish('v1/devices/me/telemetry', telemetry, quality_of_service)
            self.client.loop_stop()
        else:
            self.client.loop_start()
            while True:
                self.client.publish('v1/devices/me/telemetry', telemetry, quality_of_service)
                time.sleep(1)

    def send_attributes(self, attributes, send_nonstop=False, quality_of_service=0):
        if not send_nonstop:
            self.client.loop_start()
            self.client.publish('v1/devices/me/attributes', attributes, quality_of_service)
            self.client.loop_stop()
        else:
            self.client.loop_start()
            while True:
                ret = self.client.publish('v1/devices/me/attributes', attributes, quality_of_service)
                time.sleep(1)


    def subscribe_to_attributes(self, *attributes):
        self.attributes = attributes
        self.client.loop_start()
        self.client.subscribe('v1/devices/me/attributes', qos=2)
        self.client.loop_forever()

