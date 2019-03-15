import paho.mqtt.client as paho
import logging
import time
from json import loads

attributes_url = 'v1/devices/me/attributes'
telemetry_url = 'v1/devices/me/telemetry'
log = logging.getLogger(__name__)


class TbClient:
    class __SubscriptionInfo:
        def __init__(self, sub_id, cb):
            self.subscription_id = sub_id
            self.callback = cb

    def __init__(self, host, token):
        self.__client = paho.Client()
        self.__host = host
        self.__client.username_pw_set(token)
        self.__is_connected = False
        self.__sub_dict = {}
        self.__client.on_disconnect = None
        self.__client.on_connect = None
        self.__client.on_log = None
        self.__client.on_publish = None
        self.__client.on_message = None
        self.on_server_side_rpc_response = None
        self.__connect_callback = None
        self.__rpc_set = None

        def on_log(client, userdata, level, buf):
            log.info(buf)

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
                    self.__client.subscribe('v1/devices/me/rpc/request/+')
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
            log.info("data published")

        def on_message(client, userdata, message):
            content = loads(message.payload.decode("utf-8"))
            log.info(content)
            log.info(message.topic)
            if message.topic.startswith('v1/devices/me/rpc/request/'):
                pass
                request_id = message.topic[len('v1/devices/me/rpc/request/'):len(message.topic)]
                self.on_server_side_rpc_response(request_id, content)

            elif message.topic == attributes_url:
                message = eval(content)
                for key in self.__sub_dict.keys():
                    if self.__sub_dict.get(key):
                        for item in self.__sub_dict.get(key):
                            item.callback(message)
        self.__client.on_disconnect = on_disconnect
        self.__client.on_connect = on_connect
        self.__client.on_log = on_log
        self.__client.on_publish = on_publish
        self.__client.on_message = on_message

    def respond(self, req_id, resp, quality_of_service=1, blocking=False):
        info = self.__client.publish('v1/devices/me/rpc/response/'+req_id, resp, qos=quality_of_service)
        if blocking:
            info.wait_for_publish()

    def on_server_side_rpc_response(self, req_id, request_body):
        pass

    def set_server_side_rpc_request_handler(self, handler):
        self.__rpc_set = True
        if self.__is_connected:
            self.__client.subscribe('v1/devices/me/rpc/request/+')
        self.on_server_side_rpc_response = handler

    def __connect_callback(self, *args):
        pass

    def connect(self, callback=None, timeout=10):
        self.__client.connect(self.__host)
        self.__client.loop_start()
        self.__connect_callback = callback
        t = time.time()

        while self.__is_connected is not True:
            time.sleep(0.2)
            if time.time()-t > timeout:
                return False
        return True

    def disconnect(self):
        self.__client.disconnect()
        log.info("DISCONNECT")

    def __disconnect_callback(self, *args):
        pass

    def send_telemetry(self, telemetry, quality_of_service=0, blocking=False):
        info = self.__client.publish(telemetry_url, telemetry, quality_of_service)
        if blocking:
            info.wait_for_publish()

    def send_attributes(self, attributes, quality_of_service=0, blocking=False):
        info = self.__client.publish(attributes_url, attributes, quality_of_service)
        if blocking:
            info.wait_for_publish()

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

    def subscribe(self, key="*", quality_of_service=1, callback=None):
        self.__client.subscribe(attributes_url, qos=quality_of_service)

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
        # if attribute doesnot exist create it with subscription
        elif key not in self.__sub_dict.keys():
            self.__sub_dict.update({key: [inst]})
            log.debug("Subscribed to " + key + ", subscription id " + str(inst.subscription_id))
        # if attribute exists create subscription
        else:
            self.__sub_dict[key].append(inst)
            log.debug("Subscribed to " + key + ", subscription id " + str(inst.subscription_id))
        return inst.subscription_id
