#TODO ресайклить файл логирования время от времени
import paho.mqtt.client as paho
import logging, time
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
    is_connected = False
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
                self.is_connected = True
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

        def _on_message(client, userdata, message):
            log.info(message.payload.decode("utf-8"))
            log.info(message.topic)
            if message.topic == 'v1/devices/me/attributes':
                self.sub_message_processing({message.topic: message.payload.decode("utf-8")})


        def _on_subscribe(client, userdata, mid, granted_qos):
            log.info("Subscribe")
            #TODO добавить больше информации

        def _on_unsubscribe(client, userdata, mid, granted_qos):
            log.info("Unsubscribe")
            #TODO добавить больше информации



        self.client.on_disconnect = _on_disconnect
        self.client.on_connect = _on_connect
        self.client.on_log = _on_log
        self.client.on_publish = _on_publish
        self.client.on_message = _on_message
        self.client.on_subscribe = _on_subscribe
        self.client.on_unsubscribe = _on_unsubscribe

    def loop(self):
        return self.client.loop()

    def connect(self):
        self.client.connect(self.host)
        self.client.loop_start()
        while self.is_connected != True:  # Wait for connection
            time.sleep(0.2)


    def disconnect(self):
        self.client.disconnect()

    def send_telemetry(self, telemetry, quality_of_service=0, blocking=False):

        info = self.client.publish('v1/devices/me/telemetry', telemetry, quality_of_service)
        if blocking: info.wait_for_publish()

    def send_attributes(self, attributes, quality_of_service=0, blocking=False):

        info = self.client.publish('v1/devices/me/attributes', attributes, quality_of_service)
        if blocking: info.wait_for_publish()


    def __subscribe_to_attributes_blocking(self, callback, *attributes):
        # как мне использовать колбек?
        self.client.subscribe('v1/devices/me/attributes', qos=2)


    def subscribe_to_attributes(self, callback, attr_name):
        pass
    #def unsubscribe_to_attributes(self, subscriptionId):

    def sub_message_processing(self):
        pass

    def subscribe_to_attributes(self, callback, key="*"):
        #TODO впилить валидацию аттрибутов
        subscription_id = None
        self.attributes = key
        self.client.subscribe('v1/devices/me/attributes', qos=2)
        self.sub_message_processing = callback
        return(subscription_id)
