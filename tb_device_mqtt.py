# Copyright 2024. ThingsBoard
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#  http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from inspect import signature
from time import sleep

import paho.mqtt.client as paho
from math import ceil

try:
    from time import monotonic as time, time as timestamp
except ImportError:
    from time import time, time as timestamp
import ssl
from threading import RLock, Thread
from enum import Enum

from paho.mqtt.reasoncodes import ReasonCodes
from simplejson import loads, dumps, JSONDecodeError

from sdk_utils import verify_checksum

FW_TITLE_ATTR = "fw_title"
FW_VERSION_ATTR = "fw_version"
FW_CHECKSUM_ATTR = "fw_checksum"
FW_CHECKSUM_ALG_ATTR = "fw_checksum_algorithm"
FW_SIZE_ATTR = "fw_size"
FW_STATE_ATTR = "fw_state"

REQUIRED_SHARED_KEYS = f"{FW_CHECKSUM_ATTR},{FW_CHECKSUM_ALG_ATTR},{FW_SIZE_ATTR},{FW_TITLE_ATTR},{FW_VERSION_ATTR}"

RPC_RESPONSE_TOPIC = 'v1/devices/me/rpc/response/'
RPC_REQUEST_TOPIC = 'v1/devices/me/rpc/request/'
ATTRIBUTES_TOPIC = 'v1/devices/me/attributes'
ATTRIBUTES_TOPIC_REQUEST = 'v1/devices/me/attributes/request/'
ATTRIBUTES_TOPIC_RESPONSE = 'v1/devices/me/attributes/response/'
TELEMETRY_TOPIC = 'v1/devices/me/telemetry'
CLAIMING_TOPIC = 'v1/devices/me/claim'
PROVISION_TOPIC_REQUEST = '/provision/request'
PROVISION_TOPIC_RESPONSE = '/provision/response'
log = logging.getLogger('tb_connection')

RESULT_CODES = {
    1: "incorrect protocol version",
    2: "invalid client identifier",
    3: "server unavailable",
    4: "bad username or password",
    5: "not authorised",
}


class TBTimeoutException(Exception):
    pass


class TBQoSException(Exception):
    pass


DEFAULT_TIMEOUT = 5


class ProvisionClient(paho.Client):
    PROVISION_REQUEST_TOPIC = "/provision/request"
    PROVISION_RESPONSE_TOPIC = "/provision/response"

    def __init__(self, host, port, provision_request):
        super().__init__()
        self._host = host
        self._port = port
        self._username = "provision"
        self.__credentials = None
        self.on_connect = self.__on_connect
        self.on_message = self.__on_message
        self.__provision_request = provision_request

    def __on_connect(self, client, _, __, rc):  # Callback for connect
        if rc == 0:
            log.info("[Provisioning client] Connected to ThingsBoard ")
            client.subscribe(self.PROVISION_RESPONSE_TOPIC)  # Subscribe to provisioning response topic
            provision_request = dumps(self.__provision_request)
            log.info("[Provisioning client] Sending provisioning request %s" % provision_request)
            client.publish(self.PROVISION_REQUEST_TOPIC, provision_request)  # Publishing provisioning request topic
        else:
            log.info("[Provisioning client] Cannot connect to ThingsBoard!, result: %s" % RESULT_CODES[rc])

    def __on_message(self, _, __, msg):
        decoded_payload = msg.payload.decode("UTF-8")
        log.info("[Provisioning client] Received data from ThingsBoard: %s" % decoded_payload)
        decoded_message = loads(decoded_payload)
        provision_device_status = decoded_message.get("status")
        if provision_device_status == "SUCCESS":
            self.__credentials = decoded_message
        else:
            log.error("[Provisioning client] Provisioning was unsuccessful with status %s and message: %s" % (
                provision_device_status, decoded_message["errorMsg"]))
        self.disconnect()

    def provision(self):
        log.info("[Provisioning client] Connecting to ThingsBoard")
        self.__credentials = None
        self.connect(self._host, self._port, 60)
        self.loop_forever()

    def get_credentials(self):
        return self.__credentials


class TBSendMethod(Enum):
    SUBSCRIBE = 0
    PUBLISH = 1
    UNSUBSCRIBE = 2


class TBPublishInfo:
    TB_ERR_AGAIN = -1
    TB_ERR_SUCCESS = 0
    TB_ERR_NOMEM = 1
    TB_ERR_PROTOCOL = 2
    TB_ERR_INVAL = 3
    TB_ERR_NO_CONN = 4
    TB_ERR_CONN_REFUSED = 5
    TB_ERR_NOT_FOUND = 6
    TB_ERR_CONN_LOST = 7
    TB_ERR_TLS = 8
    TB_ERR_PAYLOAD_SIZE = 9
    TB_ERR_NOT_SUPPORTED = 10
    TB_ERR_AUTH = 11
    TB_ERR_ACL_DENIED = 12
    TB_ERR_UNKNOWN = 13
    TB_ERR_ERRNO = 14
    TB_ERR_QUEUE_SIZE = 15

    ERRORS_DESCRIPTION = {
        -1: 'Previous error repeated.',
        0: 'The operation completed successfully.',
        1: 'Out of memory.',
        2: 'A network protocol error occurred when communicating with the broker.',
        3: 'Invalid function arguments provided.',
        4: 'The client is not currently connected.',
        5: 'The connection was refused.',
        6: 'Entity not found (for example, trying to unsubscribe from a topic not currently subscribed to).',
        7: 'The connection was lost.',
        8: 'A TLS error occurred.',
        9: 'Payload size is too large.',
        10: 'This feature is not supported.',
        11: 'Authorization failed.',
        12: 'Access denied to the specified ACL.',
        13: 'Unknown error.',
        14: 'A system call returned an error.',
        15: 'The queue size was exceeded.',
        16: 'The keepalive time has been exceeded.'
    }

    def __init__(self, message_info):
        self.message_info = message_info

    # pylint: disable=invalid-name
    def rc(self):
        if isinstance(self.message_info, list):
            for info in self.message_info:
                if info.rc != 0:
                    return info.rc
            return self.TB_ERR_SUCCESS
        else:
            return self.message_info.rc

    def mid(self):
        if isinstance(self.message_info, list):
            return [info.mid for info in self.message_info]
        else:
            return self.message_info.mid

    def get(self):
        if isinstance(self.message_info, list):
            for info in self.message_info:
                info.wait_for_publish(timeout=1)
        else:
            self.message_info.wait_for_publish(timeout=1)
        return self.rc()


class RateLimit:
    def __init__(self, rate_limit):
        self.__start_time = time()
        self.__no_limit = False
        if ''.join(c for c in rate_limit if c not in [' ', ',', ';']) in ("", "0:0"):
            self.__no_limit = True
        self.__rate_limit_dict = {}
        self.__lock = RLock()
        rate_configs = rate_limit.split(";")
        if "," in rate_limit:
            rate_configs = rate_limit.split(",")
        for rate in rate_configs:
            if rate == "":
                continue
            rate = rate.split(":")
            self.__rate_limit_dict[int(rate[1])] = {"counter": 0, "start": int(time()), "limit": int(rate[0])}
        log.debug("Rate limit set to values: ")
        self.__minimal_timeout = DEFAULT_TIMEOUT * 10
        self.__minimal_limit = 1000000000
        if not self.__no_limit:
            for rate_limit_time in self.__rate_limit_dict:
                log.debug("Time: %s, Limit: %s", rate_limit_time,
                          self.__rate_limit_dict[rate_limit_time]["limit"])
                if self.__rate_limit_dict[rate_limit_time]["limit"] < self.__minimal_limit:
                    self.__minimal_limit = self.__rate_limit_dict[rate_limit_time]["limit"]
                if rate_limit_time < self.__minimal_limit:
                    self.__minimal_timeout = rate_limit_time + 1

    def increase_rate_limit_counter(self, amount=1):
        if self.__no_limit:
            return
        with self.__lock:
            for rate_limit_time in self.__rate_limit_dict:
                self.__rate_limit_dict[rate_limit_time]["counter"] += amount

    def check_limit_reached(self, amount=1):
        if self.__no_limit:
            return False
        for rate_limit_time, rate_limit_info in self.__rate_limit_dict.items():
            if self.__rate_limit_dict[rate_limit_time]["start"] + rate_limit_time <= int(time()):
                self.__rate_limit_dict[rate_limit_time]["start"] = int(time())
                self.__rate_limit_dict[rate_limit_time]["counter"] = 0
            if rate_limit_info['counter'] + amount > rate_limit_info['limit']:
                return rate_limit_time
        return False

    def get_minimal_limit(self):
        return self.__minimal_limit

    def get_minimal_timeout(self):
        return self.__minimal_timeout

    def has_limit(self):
        return not self.__no_limit

    def set_limit(self, rate_limit, percentage=0):
        rate_configs = rate_limit.split(";")
        if "," in rate_limit:
            rate_configs = rate_limit.split(",")
        for rate in rate_configs:
            if rate == "":
                continue
            rate = rate.split(":")
            self.__rate_limit_dict[int(rate[1])] = {"counter": self.__rate_limit_dict.get(int(rate[1]), {}).get('counter', 0),
                                                    "start": self.__rate_limit_dict.get(int(rate[1]), {}).get('start', int(time())),
                                                    "limit": int(rate[0]) * percentage / 100}

    @staticmethod
    def get_rate_limits_by_host(host, rate_limit, dp_rate_limit):
        rate_limit = RateLimit.get_rate_limit_by_host(host, rate_limit)
        dp_rate_limit = RateLimit.get_dp_rate_limit_by_host(host, dp_rate_limit)

        return rate_limit, dp_rate_limit

    @staticmethod
    def get_rate_limit_by_host(host, rate_limit):
        if rate_limit == "DEFAULT_TELEMETRY_RATE_LIMIT":
            if "thingsboard.cloud" in host:
                rate_limit = "100:1,4000:60,70000:3600,"
            elif "tb" in host and "cloud" in host:
                rate_limit = "10:1,300:60,7000:3600,"
            elif "demo.thingsboard.io" in host:
                rate_limit = "10:1,300:60,"
            else:
                rate_limit = "0:0,"
        elif rate_limit == "DEFAULT_MESSAGES_RATE_LIMIT":
            if "thingsboard.cloud" in host:
                rate_limit = "100:1,4000:60,70000:3600,"
            elif "tb" in host and "cloud" in host:
                rate_limit = "10:1,300:60,7000:3600,"
            elif "demo.thingsboard.io" in host:
                rate_limit = "10:1,300:60,"
            else:
                rate_limit = "0:0,"
        else:
            rate_limit = rate_limit

        return rate_limit

    @staticmethod
    def get_dp_rate_limit_by_host(host, dp_rate_limit):
        if dp_rate_limit == "DEFAULT_TELEMETRY_DP_RATE_LIMIT":
            if "thingsboard.cloud" in host:
                dp_rate_limit = "190:1,5900:60,13900:3600,"
            elif "tb" in host and "cloud" in host:
                dp_rate_limit = "15:1,500:60,13900:3600,"
            elif "demo.thingsboard.io" in host:
                dp_rate_limit = "15:1,500:60,"
            else:
                dp_rate_limit = "0:0,"
        else:
            dp_rate_limit = dp_rate_limit

        return dp_rate_limit


class TBDeviceMqttClient:
    """ThingsBoard MQTT client. This class provides interface to send data to ThingsBoard and receive data from"""

    EMPTY_RATE_LIMIT = RateLimit('0:0,')

    def __init__(self, host, port=1883, username=None, password=None, quality_of_service=None, client_id="",
                 chunk_size=0, messages_rate_limit="DEFAULT_MESSAGES_RATE_LIMIT",
                 telemetry_rate_limit="DEFAULT_TELEMETRY_RATE_LIMIT",
                 telemetry_dp_rate_limit="DEFAULT_TELEMETRY_DP_RATE_LIMIT", max_payload_size=8196, **kwargs):
        # Added for compatibility with old versions
        if kwargs.get('rate_limit') is not None or kwargs.get('dp_rate_limit') is not None:
            messages_rate_limit = messages_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', messages_rate_limit)
            telemetry_rate_limit = telemetry_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', telemetry_rate_limit)
            telemetry_dp_rate_limit = telemetry_dp_rate_limit if kwargs.get('dp_rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('dp_rate_limit', telemetry_dp_rate_limit)
        self._client = paho.Client(protocol=5, client_id=client_id)
        self.quality_of_service = quality_of_service if quality_of_service is not None else 1
        self.__host = host
        self.__port = port
        if username == "":
            log.warning("Token is not set, connection without TLS won't be established!")
        else:
            self._client.username_pw_set(username, password=password)
        self._lock = RLock()

        self._attr_request_dict = {}
        self.stopped = False
        self.__is_connected = False
        self.__device_on_server_side_rpc_response = None
        self.__connect_callback = None
        self.__device_max_sub_id = 0
        self.__device_client_rpc_number = 0
        self.__device_sub_dict = {}
        self.__device_client_rpc_dict = {}
        self.__attr_request_number = 0
        self.max_payload_size = max_payload_size
        self.service_configuration_callback = self.on_service_configuration
        telemetry_rate_limit, telemetry_dp_rate_limit = RateLimit.get_rate_limits_by_host(self.__host,
                                                                                          telemetry_rate_limit,
                                                                                          telemetry_dp_rate_limit)
        messages_rate_limit = RateLimit.get_rate_limit_by_host(self.__host, messages_rate_limit)

        self._messages_rate_limit = RateLimit(messages_rate_limit)
        self.__telemetry_rate_limit = RateLimit(telemetry_rate_limit)
        self.__telemetry_dp_rate_limit = RateLimit(telemetry_dp_rate_limit)
        self._client.max_inflight_messages_set(self.__telemetry_rate_limit.get_minimal_limit())
        self.__attrs_request_timeout = {}
        self.__timeout_thread = Thread(target=self.__timeout_check)
        self.__timeout_thread.daemon = True
        self.__timeout_thread.start()
        self._client.on_connect = self._on_connect
        self._client.on_publish = self._on_publish
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect
        self.current_firmware_info = {
            "current_" + FW_TITLE_ATTR: "Initial",
            "current_" + FW_VERSION_ATTR: "v0",
            FW_STATE_ATTR: "IDLE"
        }
        self.__request_id = 0
        self.__firmware_request_id = 0
        self.__chunk_size = chunk_size
        self.firmware_received = False
        self.__updating_thread = Thread(target=self.__update_thread, name="Updating thread")
        self.__updating_thread.daemon = True

    def _on_publish(self, client, userdata, result):
        # log.debug("Data published to ThingsBoard!")
        pass

    def _on_disconnect(self, client, userdata, result_code, properties=None):
        self.__is_connected = False
        log.warning("MQTT client was disconnected with reason code %s (%s) ",
                    str(result_code), TBPublishInfo.ERRORS_DESCRIPTION.get(result_code, "Description not found."))
        log.debug("Client: %s, user data: %s, result code: %s. Description: %s",
                  str(client), str(userdata),
                  str(result_code), TBPublishInfo.ERRORS_DESCRIPTION.get(result_code, "Description not found."))

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        if result_code == 0:
            self.__is_connected = True
            log.info("MQTT client %r - Connected!", client)
            self._subscribe_to_topic(ATTRIBUTES_TOPIC, qos=self.quality_of_service)
            self._subscribe_to_topic(ATTRIBUTES_TOPIC + "/response/+", qos=self.quality_of_service)
            self._subscribe_to_topic(RPC_REQUEST_TOPIC + '+', qos=self.quality_of_service)
            self._subscribe_to_topic(RPC_RESPONSE_TOPIC + '+', qos=self.quality_of_service)
            self.request_service_configuration(self.service_configuration_callback)
        else:
            if isinstance(result_code, int):
                if result_code in RESULT_CODES:
                    log.error("connection FAIL with error %s %s", result_code, RESULT_CODES[result_code])
                else:
                    log.error("connection FAIL with unknown error")
            elif isinstance(result_code, ReasonCodes):
                log.error("connection FAIL with error %s %s", result_code, result_code.getName())

        if callable(self.__connect_callback):
            sleep(.2)
            if "tb_client" in signature(self.__connect_callback).parameters:
                self.__connect_callback(client, userdata, flags, result_code, *extra_params, tb_client=self)
            else:
                self.__connect_callback(client, userdata, flags, result_code, *extra_params)

    def get_firmware_update(self):
        self._client.subscribe("v2/fw/response/+")
        self.send_telemetry(self.current_firmware_info)
        self.__request_firmware_info()

        self.__updating_thread.start()

    def __request_firmware_info(self):
        self.__request_id = self.__request_id + 1
        self._publish_data({"sharedKeys": REQUIRED_SHARED_KEYS},
                           f"v1/devices/me/attributes/request/{self.__request_id}",
                           1)

    def is_connected(self):
        return self.__is_connected

    def connect(self, callback=None, min_reconnect_delay=1, timeout=120, tls=False, ca_certs=None, cert_file=None,
                key_file=None, keepalive=120):
        """Connect to ThingsBoard. The callback will be called when the connection is established."""
        if tls:
            try:
                self._client.tls_set(ca_certs=ca_certs,
                                     certfile=cert_file,
                                     keyfile=key_file,
                                     cert_reqs=ssl.CERT_REQUIRED,
                                     tls_version=ssl.PROTOCOL_TLSv1_2,
                                     ciphers=None)
                self._client.tls_insecure_set(False)
            except ValueError:
                pass
        self.reconnect_delay_set(min_reconnect_delay, timeout)
        self._client.connect(self.__host, self.__port, keepalive=keepalive)
        self._client.loop_start()
        self.__connect_callback = callback

    def disconnect(self):
        """Disconnect from ThingsBoard."""
        result = self._client.disconnect()
        log.debug(self._client)
        log.debug("Disconnecting from ThingsBoard")
        self.__is_connected = False
        self._client.loop_stop()
        return result

    def stop(self):
        self.stopped = True

    def _on_message(self, client, userdata, message):
        update_response_pattern = "v2/fw/response/" + str(self.__firmware_request_id) + "/chunk/"
        if message.topic.startswith(update_response_pattern):
            firmware_data = message.payload

            self.firmware_data = self.firmware_data + firmware_data
            self.__current_chunk = self.__current_chunk + 1

            log.debug('Getting chunk with number: %s. Chunk size is : %r byte(s).' % (
                self.__current_chunk, self.__chunk_size))

            if len(self.firmware_data) == self.__target_firmware_length:
                self.__process_firmware()
            else:
                self.__get_firmware()
        else:
            content = self._decode(message)
            self._on_decoded_message(content, message)

    def _on_decoded_message(self, content, message):
        if message.topic.startswith(RPC_REQUEST_TOPIC):
            request_id = message.topic[len(RPC_REQUEST_TOPIC):len(message.topic)]
            if self.__device_on_server_side_rpc_response:
                self.__device_on_server_side_rpc_response(request_id, content)
        elif message.topic.startswith(RPC_RESPONSE_TOPIC):
            with self._lock:
                request_id = int(message.topic[len(RPC_RESPONSE_TOPIC):len(message.topic)])
                if self.__device_client_rpc_dict.get(request_id):
                    callback = self.__device_client_rpc_dict.pop(request_id)
                else:
                    callback = None
            if callback is not None:
                callback(request_id, content, None)
        elif message.topic == ATTRIBUTES_TOPIC:
            dict_results = []
            with self._lock:
                # callbacks for everything
                if self.__device_sub_dict.get("*"):
                    for subscription_id in self.__device_sub_dict["*"]:
                        dict_results.append(self.__device_sub_dict["*"][subscription_id])
                # specific callback
                keys = content.keys()
                keys_list = []
                for key in keys:
                    keys_list.append(key)
                # iterate through message
                for key in keys_list:
                    # find key in our dict
                    if self.__device_sub_dict.get(key):
                        for subscription in self.__device_sub_dict[key]:
                            dict_results.append(self.__device_sub_dict[key][subscription])
            for res in dict_results:
                res(content, None)
        elif message.topic.startswith(ATTRIBUTES_TOPIC_RESPONSE):
            with self._lock:
                req_id = int(message.topic[len(ATTRIBUTES_TOPIC + "/response/"):])
                # pop callback and use it
                if self._attr_request_dict.get(req_id):
                    callback = self._attr_request_dict.pop(req_id)
                else:
                    callback = None
            if isinstance(callback, tuple):
                callback[0](content, None, callback[1])
            elif callback is not None:
                callback(content, None)

        if message.topic.startswith("v1/devices/me/attributes"):
            self.firmware_info = loads(message.payload)
            if "/response/" in message.topic:
                self.firmware_info = self.firmware_info.get("shared", {}) if isinstance(self.firmware_info, dict) else {}
            if ((self.firmware_info.get(FW_VERSION_ATTR) is not None
                and self.firmware_info.get(FW_VERSION_ATTR) != self.current_firmware_info.get("current_" + FW_VERSION_ATTR))
                    or (self.firmware_info.get(FW_TITLE_ATTR) is not None
                        and self.firmware_info.get(FW_TITLE_ATTR) != self.current_firmware_info.get("current_" + FW_TITLE_ATTR))):
                log.debug('Firmware is not the same')
                self.firmware_data = b''
                self.__current_chunk = 0

                self.current_firmware_info[FW_STATE_ATTR] = "DOWNLOADING"
                self.send_telemetry(self.current_firmware_info)
                sleep(1)

                self.__firmware_request_id = self.__firmware_request_id + 1
                self.__target_firmware_length = self.firmware_info[FW_SIZE_ATTR]
                self.__chunk_count = 0 if not self.__chunk_size else ceil(
                    self.firmware_info[FW_SIZE_ATTR] / self.__chunk_size)
                self.__get_firmware()

    def __process_firmware(self):
        self.current_firmware_info[FW_STATE_ATTR] = "DOWNLOADED"
        self.send_telemetry(self.current_firmware_info)
        sleep(1)

        verification_result = verify_checksum(self.firmware_data, self.firmware_info.get(FW_CHECKSUM_ALG_ATTR),
                                              self.firmware_info.get(FW_CHECKSUM_ATTR))

        if verification_result:
            log.debug('Checksum verified!')
            self.current_firmware_info[FW_STATE_ATTR] = "VERIFIED"
            self.send_telemetry(self.current_firmware_info)
            sleep(1)
        else:
            log.debug('Checksum verification failed!')
            self.current_firmware_info[FW_STATE_ATTR] = "FAILED"
            self.send_telemetry(self.current_firmware_info)
            self.__request_firmware_info()
            return
        self.firmware_received = True

    def __get_firmware(self):
        payload = '' if not self.__chunk_size or self.__chunk_size > self.firmware_info.get(FW_SIZE_ATTR, 0) \
            else str(self.__chunk_size).encode()
        self._publish_data(payload, f"v2/fw/request/{self.__firmware_request_id}/chunk/{self.__current_chunk}",
                           1)

    def __on_firmware_received(self, version_to):
        with open(self.firmware_info.get(FW_TITLE_ATTR), "wb") as firmware_file:
            firmware_file.write(self.firmware_data)
        log.info('Firmware is updated!\n Current firmware version is: %s' % version_to)

    def __update_thread(self):
        while True:
            if self.firmware_received:
                self.current_firmware_info[FW_STATE_ATTR] = "UPDATING"
                self.send_telemetry(self.current_firmware_info)
                sleep(1)

                self.__on_firmware_received(self.firmware_info.get(FW_VERSION_ATTR))

                self.current_firmware_info = {
                    "current_" + FW_TITLE_ATTR: self.firmware_info.get(FW_TITLE_ATTR),
                    "current_" + FW_VERSION_ATTR: self.firmware_info.get(FW_VERSION_ATTR),
                    FW_STATE_ATTR: "UPDATED"
                }
                self.send_telemetry(self.current_firmware_info)
                self.firmware_received = False

            sleep(0.2)

    @staticmethod
    def _decode(message):
        try:
            if isinstance(message.payload, bytes):
                content = loads(message.payload.decode("utf-8", "ignore"))
            else:
                content = loads(message.payload)
        except JSONDecodeError:
            try:
                content = message.payload.decode("utf-8", "ignore")
            except JSONDecodeError:
                content = message.payload
        return content

    def max_inflight_messages_set(self, inflight):
        """Set the maximum number of messages with QoS>0 that can be a part way through their network flow at once.
        Defaults to minimal rate limit. Increasing this value will consume more memory but can increase throughput."""
        self._client.max_inflight_messages_set(inflight)

    def max_queued_messages_set(self, queue_size):
        """Set the maximum number of outgoing messages with QoS>0 that can be pending in the outgoing message queue.
        Defaults to 0. 0 means unlimited. When the queue is full, any further outgoing messages would be dropped."""
        self._client.max_queued_messages_set(queue_size)

    def reconnect_delay_set(self, min_delay=1, max_delay=120):
        """The client will automatically retry connection. Between each attempt it will wait a number of seconds
         between min_delay and max_delay. When the connection is lost, initially the reconnection attempt is delayed
         of min_delay seconds. It’s doubled between subsequent attempt up to max_delay. The delay is reset to min_delay
          when the connection complete (e.g. the CONNACK is received, not just the TCP connection is established)."""
        self._client.reconnect_delay_set(min_delay, max_delay)

    def send_rpc_reply(self, req_id, resp, quality_of_service=None, wait_for_publish=False):
        """Send RPC reply to ThingsBoard. The response will be sent to the RPC_RESPONSE_TOPIC with the request id."""
        quality_of_service = quality_of_service if quality_of_service is not None else self.quality_of_service
        if quality_of_service not in (0, 1):
            log.error("Quality of service (qos) value must be 0 or 1")
            return None
        info = self._publish_data(resp, RPC_RESPONSE_TOPIC + req_id, quality_of_service)
        if wait_for_publish:
            info.get()

    def send_rpc_call(self, method, params, callback):
        """Send RPC call to ThingsBoard. The callback will be called when the response is received."""
        with self._lock:
            self.__device_client_rpc_number += 1
            self.__device_client_rpc_dict.update({self.__device_client_rpc_number: callback})
            rpc_request_id = self.__device_client_rpc_number
        payload = {"method": method, "params": params}
        self._publish_data(payload,
                           RPC_REQUEST_TOPIC + str(rpc_request_id),
                           self.quality_of_service)

    def request_service_configuration(self, callback):
        self.send_rpc_call("getSessionLimits", {}, callback)

    def on_service_configuration(self, _, service_config, *args, **kwargs):
        if not isinstance(service_config, dict) or 'rateLimit' not in service_config:
            log.warning("Cannot retrieve service configuration, session will use default configuration.")
            log.debug("Received the following response: %r", service_config)
            return
        if service_config.get("rateLimit"):
            rate_limits_config = service_config.get("rateLimit")

            if rate_limits_config.get('messages'):
                self._messages_rate_limit.set_limit(rate_limits_config.get('messages'), percentage=80)
            if rate_limits_config.get('telemetryMessages'):
                self.__telemetry_rate_limit.set_limit(rate_limits_config.get('telemetryMessages'), percentage=80)
            if rate_limits_config.get('telemetryDataPoints'):
                self.__telemetry_dp_rate_limit.set_limit(rate_limits_config.get('telemetryDataPoints'), percentage=80)
        if service_config.get('maxInflightMessages'):
            self.max_inflight_messages_set(int(service_config.get('maxInflightMessages')))
        if service_config.get('maxPayloadSize'):
            self.max_payload_size = int(service_config.get('maxPayloadSize'))
        log.info("Service configuration was successfully retrieved and applied.")
        log.info("Current limits: %r", service_config)

    def set_server_side_rpc_request_handler(self, handler):
        """Set the callback that will be called when a server-side RPC is received."""
        self.__device_on_server_side_rpc_response = handler

    def _wait_for_rate_limit_released(self, timeout, message_rate_limit, dp_rate_limit=None, amount=1):
        start_time = int(time())
        dp_rate_limit_timeout = dp_rate_limit.get_minimal_timeout() if dp_rate_limit is not None else 0
        timeout = max(message_rate_limit.get_minimal_timeout(), dp_rate_limit_timeout, timeout) + 10
        timeout_updated = False
        disconnected = False
        limit_reached_check = True
        log_posted = False
        while limit_reached_check:
            limit_reached_check = (message_rate_limit.check_limit_reached()
                                   or (dp_rate_limit is not None and dp_rate_limit.check_limit_reached(amount=amount))
                                   or not self.is_connected())
            if not timeout_updated and limit_reached_check:
                timeout = max(timeout, limit_reached_check) + 10
                timeout_updated = True
            if self.stopped:
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if not disconnected and not self.is_connected():
                log.warning("Waiting for connection to be established before sending data to ThingsBoard!")
                disconnected = True
                timeout = max(timeout, 180) + 10
            if int(time()) >= timeout + start_time:
                log.error("Timeout while waiting for rate limit to be released!")
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if not log_posted and limit_reached_check:
                if isinstance(limit_reached_check, int):
                    log.debug("Rate limit reached for %i seconds, waiting for rate limit to be released...", limit_reached_check)
                else:
                    log.debug("Waiting for rate limit to be released...")
                log_posted = True
            sleep(.001)

    def wait_until_current_queued_messages_processed(self):
        previous_notification_time = 0
        while self._client._out_messages and not self.stopped:
            if int(time()) - previous_notification_time > 5:
                log.debug("Waiting for all messages to be processed by paho client...")
                previous_notification_time = int(time())
            sleep(.001)

    def _send_request(self, _type, kwargs, timeout=DEFAULT_TIMEOUT, device=None,
                      msg_rate_limit=None, dp_rate_limit=None):
        if msg_rate_limit is None:
            if kwargs.get('topic') == TELEMETRY_TOPIC:
                msg_rate_limit = self.__telemetry_rate_limit
            else:
                msg_rate_limit = self._messages_rate_limit
        if dp_rate_limit is None:
            if kwargs.get('topic') == TELEMETRY_TOPIC:
                dp_rate_limit = self.__telemetry_dp_rate_limit
            else:
                dp_rate_limit = self.EMPTY_RATE_LIMIT
        if msg_rate_limit.has_limit() or dp_rate_limit.has_limit():
            msg_rate_limit.increase_rate_limit_counter()
            is_reached = self._wait_for_rate_limit_released(timeout, msg_rate_limit, dp_rate_limit)
            if is_reached:
                return is_reached

        if _type == TBSendMethod.PUBLISH:
            if msg_rate_limit.has_limit():
                self.__add_metadata_to_data_dict_from_device(kwargs["payload"])
                return self.__send_publish_with_limitations(kwargs, timeout, device, msg_rate_limit, dp_rate_limit)
            else:
                if "payload" in kwargs:
                    not_converted_to_str = True
                    if isinstance(kwargs["payload"], dict):
                        self.__add_metadata_to_data_dict_from_device(kwargs["payload"])
                        kwargs["payload"] = dumps(kwargs["payload"])
                        not_converted_to_str = False
                    elif isinstance(kwargs["payload"], str):
                        if 'metadata' in kwargs["payload"]:
                            payload = loads(kwargs["payload"])
                            self.__add_metadata_to_data_dict_from_device(payload)
                            not_converted_to_str = False
                    if not_converted_to_str and not isinstance(kwargs["payload"], str):
                        kwargs["payload"] = dumps(kwargs["payload"])
                return TBPublishInfo(self._client.publish(**kwargs))
        elif _type == TBSendMethod.SUBSCRIBE:
            return self._client.subscribe(**kwargs)
        elif _type == TBSendMethod.UNSUBSCRIBE:
            return self._client.unsubscribe(**kwargs)

    def __add_metadata_to_data_dict_from_device(self, data):
        if isinstance(data, dict) and "metadata" in data:
            data["metadata"]["publishedTs"] = int(timestamp() * 1000)
        elif isinstance(data, list):
            current_time = int(timestamp() * 1000)
            for data_item in data:
                if isinstance(data_item, dict):
                    if 'ts' in data_item and 'metadata' in data_item:
                        data_item["metadata"]["publishedTs"] = current_time
        elif isinstance(data, dict):
            for key, value in data.items():
                self.__add_metadata_to_data_dict_from_device(value)

    def __get_rate_limits_by_topic(self, topic, device=None, msg_rate_limit=None, dp_rate_limit=None):
        if device is not None:
            return msg_rate_limit, dp_rate_limit
        else:
            if topic == TELEMETRY_TOPIC:
                return self.__telemetry_rate_limit, self.__telemetry_dp_rate_limit
            else:
                return self._messages_rate_limit, None

    def __send_publish_with_limitations(self, kwargs, timeout, device=None, msg_rate_limit: RateLimit = None,
                                        dp_rate_limit: RateLimit = None):
        data = kwargs.get("payload")
        if isinstance(data, str):
            data = loads(data)
        datapoints = -1
        if dp_rate_limit.has_limit():
            datapoints = self._count_datapoints_in_message(data, device=device)
        payload = data
        if dp_rate_limit.has_limit() and datapoints >= 0 and dp_rate_limit.get_minimal_limit() < datapoints:
            log.debug("Rate limit is too low, cannot send message with %i datapoints, "
                      "splitting to messages with %i datapoints",
                      datapoints, dp_rate_limit.get_minimal_limit())
            if device is None or data.get(device) is None:
                device_split_messages = self._split_message(data, dp_rate_limit.get_minimal_limit(),
                                                            self.max_payload_size)
                split_messages = [{'message': split_message['data'], 'datapoints': split_message['datapoints']}
                                  for split_message in device_split_messages]
            else:
                device_data = data.get(device)
                device_split_messages = self._split_message(device_data, dp_rate_limit.get_minimal_limit(),
                                                            self.max_payload_size)
                split_messages = [
                    {'message': {device: [split_message['data']]}, 'datapoints': split_message['datapoints'],
                     'metadata': split_message.get('metadata')} for split_message in device_split_messages]

            if len(split_messages) == 0:
                log.debug("Cannot split message to smaller parts!")

            results = []
            for part in split_messages:
                dp_rate_limit.increase_rate_limit_counter(part['datapoints'])
                self._wait_for_rate_limit_released(timeout,
                                                   message_rate_limit=msg_rate_limit,
                                                   dp_rate_limit=dp_rate_limit,
                                                   amount=dp_rate_limit.get_minimal_limit())
                kwargs["payload"] = dumps(part['message'])
                self.wait_until_current_queued_messages_processed()
                results.append(self._client.publish(**kwargs))
            return TBPublishInfo(results)
        else:
            if dp_rate_limit is not None:
                dp_rate_limit.increase_rate_limit_counter(datapoints)
                self._wait_for_rate_limit_released(timeout,
                                                   message_rate_limit=msg_rate_limit,
                                                   dp_rate_limit=dp_rate_limit,
                                                   amount=datapoints)
            kwargs["payload"] = dumps(payload)
            return TBPublishInfo(self._client.publish(**kwargs))

    def _subscribe_to_topic(self, topic, qos=None, timeout=DEFAULT_TIMEOUT):
        if qos is None:
            qos = self.quality_of_service

        waiting_for_connection_message_time = 0
        while not self.is_connected() and not self.stopped:
            if self.stopped:
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if time() - waiting_for_connection_message_time > 10.0:
                log.warning("Waiting for connection to be established before subscribing for data on ThingsBoard!")
                waiting_for_connection_message_time = time()
            sleep(0.01)

        return self._send_request(TBSendMethod.SUBSCRIBE, {"topic": topic, "qos": qos}, timeout, msg_rate_limit=self._messages_rate_limit)

    def _publish_data(self, data, topic, qos, timeout=DEFAULT_TIMEOUT, device=None,
                      msg_rate_limit=None, dp_rate_limit=None):
        if qos is None:
            qos = self.quality_of_service
        if qos not in (0, 1):
            log.exception("Quality of service (qos) value must be 0 or 1")
            raise TBQoSException("Quality of service (qos) value must be 0 or 1")

        waiting_for_connection_message_time = 0
        while not self.is_connected():
            if self.stopped:
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if time() - waiting_for_connection_message_time > 10.0:
                log.warning("Waiting for connection to be established before sending data to ThingsBoard!")
                waiting_for_connection_message_time = time()
            sleep(0.01)

        return self._send_request(TBSendMethod.PUBLISH, {"topic": topic, "payload": data, "qos": qos}, timeout,
                                  device=device, msg_rate_limit=msg_rate_limit, dp_rate_limit=dp_rate_limit)

    def send_telemetry(self, telemetry, quality_of_service=None, wait_for_publish=True):
        """Send telemetry to ThingsBoard. The telemetry can be a single dictionary or a list of dictionaries."""
        quality_of_service = quality_of_service if quality_of_service is not None else self.quality_of_service
        if not isinstance(telemetry, list) and not (isinstance(telemetry, dict) and telemetry.get("ts") is not None):
            telemetry = [telemetry]
        return self._publish_data(telemetry, TELEMETRY_TOPIC, quality_of_service, wait_for_publish)

    def send_attributes(self, attributes, quality_of_service=None, wait_for_publish=True):
        """Send attributes to ThingsBoard. The attributes can be a single dictionary or a list of dictionaries."""
        quality_of_service = quality_of_service if quality_of_service is not None else self.quality_of_service
        return self._publish_data(attributes, ATTRIBUTES_TOPIC, quality_of_service, wait_for_publish)

    def unsubscribe_from_attribute(self, subscription_id):
        """Unsubscribe from attribute updates for subscription_id."""
        with self._lock:
            for attribute in self.__device_sub_dict:
                if self.__device_sub_dict[attribute].get(subscription_id):
                    del self.__device_sub_dict[attribute][subscription_id]
                    log.debug("Unsubscribed from %s, subscription id %i", attribute, subscription_id)
            if subscription_id == '*':
                self.__device_sub_dict = {}
            self.__device_sub_dict = dict((k, v) for k, v in self.__device_sub_dict.items() if v)

    def clean_device_sub_dict(self):
        self.__device_sub_dict = {}

    def subscribe_to_all_attributes(self, callback):
        """Subscribe to all attribute updates. The callback will be called when an attribute update is received."""
        return self.subscribe_to_attribute("*", callback)

    def subscribe_to_attribute(self, key, callback):
        """Subscribe to attribute updates for attribute with key.
        The callback will be called when an attribute update is received."""
        with self._lock:
            self.__device_max_sub_id += 1
            if key not in self.__device_sub_dict:
                self.__device_sub_dict.update({key: {self.__device_max_sub_id: callback}})
            else:
                self.__device_sub_dict[key].update({self.__device_max_sub_id: callback})
            log.debug("Subscribed to %s with id %i", key, self.__device_max_sub_id)
            return self.__device_max_sub_id

    def request_attributes(self, client_keys=None, shared_keys=None, callback=None):
        """Request attributes from ThingsBoard. The callback will be called when the response is received."""
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

        ts_in_millis = int(time())

        attr_request_number = self._add_attr_request_callback(callback)

        info = self._publish_data(msg, ATTRIBUTES_TOPIC_REQUEST + str(attr_request_number), self.quality_of_service)

        self._add_timeout(attr_request_number, ts_in_millis, timeout=20)
        return info

    def _add_timeout(self, attr_request_number, timestamp, timeout=DEFAULT_TIMEOUT):
        timestamp += timeout
        self.__attrs_request_timeout[attr_request_number] = int(timestamp)

    def _add_attr_request_callback(self, callback):
        with self._lock:
            self.__attr_request_number += 1
            self._attr_request_dict.update({self.__attr_request_number: callback})
            attr_request_number = self.__attr_request_number
        return attr_request_number

    def __timeout_check(self):
        while not self.stopped:
            current_ts_in_millis = int(time())
            for (attr_request_number, ts) in tuple(self.__attrs_request_timeout.items()):
                if current_ts_in_millis < ts:
                    continue

                with self._lock:
                    callback = None
                    if self._attr_request_dict.get(attr_request_number):
                        callback = self._attr_request_dict.pop(attr_request_number)

                if callback is not None:
                    if isinstance(callback, tuple):
                        callback[0](None, TBTimeoutException("Timeout while waiting for a reply from ThingsBoard!"),
                                    callback[1])
                    else:
                        callback(None, TBTimeoutException("Timeout while waiting for a reply from ThingsBoard!"))

                self.__attrs_request_timeout.pop(attr_request_number)

            sleep(0.2)

    def claim(self, secret_key, duration=30000):
        """Claim the device in Thingsboard. The duration is in milliseconds."""
        claiming_request = {
            "secretKey": secret_key,
            "durationMs": duration
        }
        info = self._publish_data(claiming_request, CLAIMING_TOPIC, self.quality_of_service)
        return info

    @staticmethod
    def _count_datapoints_in_message(data, device=None):
        datapoints = 0
        if device is not None:
            if isinstance(data.get(device), list):
                for data_object in data[device]:
                    datapoints += TBDeviceMqttClient._count_datapoints_in_message(data_object)  # noqa
            else:
                datapoints += TBDeviceMqttClient._count_datapoints_in_message(data.get(device, data.get('device')))
        else:
            if isinstance(data, dict):
                datapoints += TBDeviceMqttClient._get_data_points_from_message(data)
            else:
                for item in data:
                    datapoints += TBDeviceMqttClient._get_data_points_from_message(item)
        return datapoints

    @staticmethod
    def _get_data_points_from_message(data):
        if isinstance(data, dict):
            if data.get("ts") is not None and data.get("values") is not None:
                datapoints_in_message_amount = len(data['values']) + len(str(data['values']))/1000
            else:
                datapoints_in_message_amount = len(data.keys()) + len(str(data))/1000
        else:
            datapoints_in_message_amount = len(data) + len(str(data))/1000
        return int(datapoints_in_message_amount)

    @staticmethod
    def provision(host,
                  provision_device_key,
                  provision_device_secret,
                  port=1883,
                  device_name=None,
                  access_token=None,
                  client_id=None,
                  username=None,
                  password=None,
                  hash=None,
                  gateway=None):
        """Provision the device in ThingsBoard. Returns the credentials for the device."""
        provision_request = {
            "provisionDeviceKey": provision_device_key,
            "provisionDeviceSecret": provision_device_secret
        }

        if access_token is not None:
            provision_request["token"] = access_token
            provision_request["credentialsType"] = "ACCESS_TOKEN"
        elif username is not None or password is not None or client_id is not None:
            provision_request["username"] = username
            provision_request["password"] = password
            provision_request["clientId"] = client_id
            provision_request["credentialsType"] = "MQTT_BASIC"
        elif hash is not None:
            provision_request["hash"] = hash
            provision_request["credentialsType"] = "X509_CERTIFICATE"

        if device_name is not None:
            provision_request["deviceName"] = device_name

        if gateway is not None:
            provision_request["gateway"] = gateway

        provisioning_client = ProvisionClient(host=host, port=port, provision_request=provision_request)
        provisioning_client.provision()
        return provisioning_client.get_credentials()

    @staticmethod
    def _split_message(message_pack, max_size, max_payload_size):
        if message_pack is None:
            return []
        split_messages = []
        if isinstance(message_pack, dict) and message_pack.get('device') is not None and len(message_pack.keys()) in [1,2]:
            return [{'data': message_pack, 'datapoints': TBDeviceMqttClient._count_datapoints_in_message(message_pack), 'message': message_pack}]
        if not isinstance(message_pack, list):
            message_pack = [message_pack]
        final_message_item = {'data': {}, 'datapoints': 0}
        add_last_item = False
        for message in message_pack:
            if isinstance(message, dict):
                ts = None
                if message.get("ts") is not None and message.get("values") is not None:
                    ts = message.get("ts")
                    values = message.get("values")
                else:
                    values = message

                values_data_keys = tuple(values.keys())
                if len(values_data_keys) == 1:
                    if ts is not None:
                        final_message_item['data']['ts'] = ts
                        final_message_item['data']['values'] = values
                    if final_message_item['datapoints'] < max_size:
                        if ts is not None:
                            final_message_item['data']['values'].update(values)
                            final_message_item['datapoints'] += 1
                        else:
                            final_message_item['data'].update(values)
                            final_message_item['datapoints'] += 1
                            continue
                    else:
                        split_messages.append(final_message_item)
                        final_message_item = {'data': {**values}, 'datapoints': 0}  # Copy is required,
                        # because we need to keep the original dict for next iteration
                        add_last_item = True
                        continue
                message_item_values_with_allowed_size = {}
                for current_data_key_index in range(len(values_data_keys)):
                    data_key = values_data_keys[current_data_key_index]
                    if len(message_item_values_with_allowed_size.keys()) < max_size and len(
                            str(message_item_values_with_allowed_size)) < max_payload_size:
                        message_item_values_with_allowed_size[data_key] = values[data_key]
                    if (len(message_item_values_with_allowed_size.keys()) >= max_size
                        or current_data_key_index == len(values_data_keys) - 1) or len(
                            str(message_item_values_with_allowed_size)) >= max_payload_size:
                        if ts is not None:
                            final_message_item['data'] = {"ts": ts, "values": message_item_values_with_allowed_size,
                                                          'metadata': message.get('metadata')}
                        else:
                            final_message_item['data'] = message_item_values_with_allowed_size
                        final_message_item['datapoints'] = len(message_item_values_with_allowed_size)
                        split_messages.append({**final_message_item})  # Copy is required,
                        # because we need to keep the original dict for next iteration
                        message_item_values_with_allowed_size = {}
            else:
                log.error("Message is not a dictionary!")
                log.debug("Message: %s", message)
        if add_last_item:
            split_messages.append(final_message_item)
        return split_messages
