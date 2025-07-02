# Copyright 2025. ThingsBoard
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
from copy import deepcopy
from inspect import signature
from time import sleep
from importlib import metadata

from orjson.orjson import OPT_NON_STR_KEYS

from utils import install_package
from os import environ

def check_tb_paho_mqtt_installed():
    try:
        dists = metadata.distributions()
        for dist in dists:
            if dist.metadata["Name"].lower() == "tb-paho-mqtt-client":
                files = list(dist.files)
                for file in files:
                    if str(file).startswith("paho/mqtt"):
                        return True
        return False
    except Exception:
        return False

if not check_tb_paho_mqtt_installed():
    try:
        install_package('tb-paho-mqtt-client', version='>=2.1.2')
    except Exception as e:
        raise ImportError("tb-paho-mqtt-client is not installed, please install it manually.") from e

import paho.mqtt.client as paho
from paho.mqtt.enums import CallbackAPIVersion
from math import ceil

try:
    from time import monotonic, time as timestamp
except ImportError:
    from time import time as timestamp
import ssl
from threading import RLock, Thread
from enum import Enum

from paho.mqtt.reasoncodes import ReasonCodes
from paho.mqtt.client import MQTT_ERR_QUEUE_SIZE

from orjson import dumps, loads, JSONDecodeError

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
    5: "not authorized",
}


class TBTimeoutException(Exception):
    pass


class TBQoSException(Exception):
    pass


DEFAULT_TIMEOUT = 5


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
                if isinstance(info.rc, ReasonCodes):
                    if info.rc.value == 0:
                        continue
                    return info.rc
                else:
                    if info.rc != 0:
                        return info.rc
            return self.TB_ERR_SUCCESS
        else:
            if isinstance(self.message_info.rc, ReasonCodes):
                return self.message_info.rc.value
            return self.message_info.rc

    def mid(self):
        if isinstance(self.message_info, list):
            return [info.mid for info in self.message_info]
        else:
            return self.message_info.mid

    def get(self):
        if isinstance(self.message_info, list):
            try:
                for info in self.message_info:
                    info.wait_for_publish(timeout=1)
            except Exception as e:
                global log
                log = logging.getLogger('tb_connection')
                log.error("Error while waiting for publish: %s", e)
        else:
            self.message_info.wait_for_publish(timeout=1)
        return self.rc()


class GreedyTokenBucket:
    def __init__(self, capacity, duration_sec):
        self.capacity = float(capacity)
        self.duration = float(duration_sec)
        self.tokens = float(capacity)
        self.last_updated = monotonic()

    def refill(self):
        now = monotonic()
        elapsed = now - self.last_updated
        refill_rate = self.capacity / self.duration
        refill_amount = elapsed * refill_rate
        self.tokens = min(self.capacity, self.tokens + refill_amount)
        self.last_updated = now

    def can_consume(self, amount=1):
        self.refill()
        return round(self.tokens, 6) >= round(amount, 6)

    def consume(self, amount=1):
        self.refill()
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False

    def get_remaining_tokens(self):
        self.refill()
        return self.tokens


DEFAULT_RATE_LIMIT_PERCENTAGE = environ.get('TB_DEFAULT_RATE_LIMIT_PERCENTAGE')
if DEFAULT_RATE_LIMIT_PERCENTAGE is None:
    DEFAULT_RATE_LIMIT_PERCENTAGE = 80
else:
    try:
        DEFAULT_RATE_LIMIT_PERCENTAGE = int(DEFAULT_RATE_LIMIT_PERCENTAGE)
    except ValueError:
        log.warning("Invalid value for TB_DEFAULT_RATE_LIMIT_PERCENTAGE, using default value of 80%%")
        DEFAULT_RATE_LIMIT_PERCENTAGE = 80

class RateLimit:
    def __init__(self, rate_limit, name=None, percentage=DEFAULT_RATE_LIMIT_PERCENTAGE):
        self.__reached_limit_index = 0
        self.__reached_limit_index_time = 0
        self._no_limit = False
        self._rate_buckets = {}
        self.__lock = RLock()
        self._minimal_timeout = DEFAULT_TIMEOUT
        self._minimal_limit = float("inf")

        from_dict = isinstance(rate_limit, dict)
        self.name = name
        self.percentage = percentage

        if from_dict:
            self._no_limit = rate_limit.get('no_limit', False)
            self.percentage = rate_limit.get('percentage', percentage)
            self.name = rate_limit.get('name', name)

            rate_limits = rate_limit.get('rateLimits', {})
            for duration_str, bucket_info in rate_limits.items():
                try:
                    duration = int(duration_str)
                    capacity = bucket_info.get("capacity")
                    tokens = bucket_info.get("tokens")
                    last_updated = bucket_info.get("last_updated")

                    if capacity is None or tokens is None:
                        continue

                    bucket = GreedyTokenBucket(capacity, duration)
                    bucket.tokens = min(capacity, float(tokens))
                    bucket.last_updated = float(last_updated) if last_updated is not None else monotonic()

                    self._rate_buckets[duration] = bucket
                    self._minimal_limit = min(self._minimal_limit, capacity)
                    self._minimal_timeout = min(self._minimal_timeout, duration + 1)
                except Exception as e:
                    log.warning("Invalid bucket format for duration %s: %s", duration_str, e)

        else:
            clean = ''.join(c for c in rate_limit if c not in [' ', ',', ';'])
            if clean in ("", "0:0"):
                self._no_limit = True
                return

            rate_configs = rate_limit.replace(";", ",").split(",")
            for rate in rate_configs:
                if not rate.strip():
                    continue
                try:
                    limit_str, duration_str = rate.strip().split(":")
                    limit = int(int(limit_str) * self.percentage / 100)
                    duration = int(duration_str)
                    bucket = GreedyTokenBucket(limit, duration)
                    self._rate_buckets[duration] = bucket
                    self._minimal_limit = min(self._minimal_limit, limit)
                    self._minimal_timeout = min(self._minimal_timeout, duration + 1)
                except Exception as e:
                    log.warning("Invalid rate limit format '%s': %s", rate, e)

        log.debug("Rate limit %s set to values:", self.name)
        for duration, bucket in self._rate_buckets.items():
            log.debug("Window: %ss, Limit: %s", duration, bucket.capacity)

    def increase_rate_limit_counter(self, amount=1):
        if self._no_limit:
            return
        with self.__lock:
            for bucket in self._rate_buckets.values():
                bucket.refill()
                bucket.tokens = max(0.0, bucket.tokens - amount)

    def check_limit_reached(self, amount=1):
        if self._no_limit:
            return False
        with self.__lock:
            for duration, bucket in self._rate_buckets.items():
                if not bucket.can_consume(amount):
                    return bucket.capacity, duration

            for duration, bucket in self._rate_buckets.items():
                log.debug("%s left tokens: %.2f per %r seconds",
                          self.name,
                          bucket.get_remaining_tokens(),
                          duration)

            return False


    def get_minimal_limit(self):
        return self._minimal_limit if self.has_limit() else 0

    def get_minimal_timeout(self):
        return self._minimal_timeout if self.has_limit() else 0

    def has_limit(self):
        return not self._no_limit

    def set_limit(self, rate_limit, percentage=DEFAULT_RATE_LIMIT_PERCENTAGE):
        with self.__lock:
            self._minimal_timeout = DEFAULT_TIMEOUT
            self._minimal_limit = float("inf")

            old_buckets = deepcopy(self._rate_buckets)
            self._rate_buckets = {}
            self.percentage = percentage if percentage > 0 else self.percentage

            clean = ''.join(c for c in rate_limit if c not in [' ', ',', ';'])
            if clean in ("", "0:0"):
                self._no_limit = True
                return

            rate_configs = rate_limit.replace(";", ",").split(",")

            for rate in rate_configs:
                if not rate.strip():
                    continue
                try:
                    limit_str, duration_str = rate.strip().split(":")
                    duration = int(duration_str)
                    new_capacity = int(int(limit_str) * self.percentage / 100)

                    previous_bucket = old_buckets.get(duration)
                    new_bucket = GreedyTokenBucket(new_capacity, duration)

                    if previous_bucket:
                        previous_bucket.refill()
                        used = max(0.0, previous_bucket.capacity - previous_bucket.tokens)
                        new_tokens = new_capacity - used
                        new_bucket.tokens = min(new_capacity, max(0.0, new_tokens))
                        new_bucket.last_updated = monotonic()
                    else:
                        new_bucket.tokens = new_capacity
                        new_bucket.last_updated = monotonic()

                    self._rate_buckets[duration] = new_bucket
                    self._minimal_limit = min(self._minimal_limit, new_bucket.capacity)
                    self._minimal_timeout = min(self._minimal_timeout, duration + 1)

                except Exception as e:
                    log.warning("Invalid rate limit format '%s': %s", rate, e)

            self._no_limit = not bool(self._rate_buckets)
            log.debug("Rate limit set to values:")
            for duration, bucket in self._rate_buckets.items():
                log.debug("Duration: %ss, Limit: %s", duration, bucket.capacity)

    def reach_limit(self):
        if self._no_limit or not self._rate_buckets:
            return

        with self.__lock:
            durations = sorted(self._rate_buckets.keys())
            current_monotonic = monotonic()
            if self.__reached_limit_index_time >= current_monotonic - self._rate_buckets[durations[-1]].duration:
                self.__reached_limit_index = 0
                self.__reached_limit_index_time = current_monotonic
            if self.__reached_limit_index >= len(durations):
                self.__reached_limit_index = 0
                self.__reached_limit_index_time = current_monotonic

            target_duration = durations[self.__reached_limit_index]
            bucket = self._rate_buckets[target_duration]
            bucket.refill()
            bucket.tokens = 0.0

            self.__reached_limit_index += 1
        log.info("Received disconnection due to rate limit for \"%s\" rate limit, waiting for tokens in bucket for %s seconds",
                 self.name,
                 target_duration)
        return self.__reached_limit_index, self.__reached_limit_index_time

    @property
    def __dict__(self):
        rate_limits_dict = {}
        for duration, bucket in self._rate_buckets.items():
            rate_limits_dict[str(duration)] = {
                "capacity": bucket.capacity,
                "tokens": bucket.get_remaining_tokens(),
                "last_updated": bucket.last_updated
            }
        return {
            "rateLimits": rate_limits_dict,
            "name": self.name,
            "percentage": self.percentage,
            "no_limit": self._no_limit
        }

    @staticmethod
    def get_rate_limits_by_host(host, rate_limit, dp_rate_limit):
        rate_limit = RateLimit.get_rate_limit_by_host(host, rate_limit)
        dp_rate_limit = RateLimit.get_dp_rate_limit_by_host(host, dp_rate_limit)

        return rate_limit, dp_rate_limit

    @staticmethod
    def get_rate_limit_by_host(host, rate_limit):
        if rate_limit == "DEFAULT_TELEMETRY_RATE_LIMIT":
            if "thingsboard.cloud" in host:
                rate_limit = "10:1,60:60,"
            elif "tb" in host and "cloud" in host:
                rate_limit = "10:1,60:60,"
            elif "demo.thingsboard.io" in host:
                rate_limit = "10:1,60:60,"
            else:
                rate_limit = "0:0,"
        elif rate_limit == "DEFAULT_MESSAGES_RATE_LIMIT":
            if "thingsboard.cloud" in host:
                rate_limit = "10:1,60:60,"
            elif "tb" in host and "cloud" in host:
                rate_limit = "10:1,60:60,"
            elif "demo.thingsboard.io" in host:
                rate_limit = "10:1,60:60,"
            else:
                rate_limit = "0:0,"
        else:
            rate_limit = rate_limit

        return rate_limit

    @staticmethod
    def get_dp_rate_limit_by_host(host, dp_rate_limit):
        if dp_rate_limit == "DEFAULT_TELEMETRY_DP_RATE_LIMIT":
            if "thingsboard.cloud" in host:
                dp_rate_limit = "10:1,300:60,"
            elif "tb" in host and "cloud" in host:
                dp_rate_limit = "10:1,300:60,"
            elif "demo.thingsboard.io" in host:
                dp_rate_limit = "10:1,300:60,"
            else:
                dp_rate_limit = "0:0,"
        else:
            dp_rate_limit = dp_rate_limit

        return dp_rate_limit


class TBDeviceMqttClient:
    """ThingsBoard MQTT client. This class provides interface to send data to ThingsBoard and receive data from"""

    EMPTY_RATE_LIMIT = RateLimit('0:0,', "EMPTY_RATE_LIMIT")

    def __init__(self, host, port=1883, username=None, password=None, quality_of_service=None, client_id="",
                 chunk_size=0, messages_rate_limit="DEFAULT_MESSAGES_RATE_LIMIT",
                 telemetry_rate_limit="DEFAULT_TELEMETRY_RATE_LIMIT",
                 telemetry_dp_rate_limit="DEFAULT_TELEMETRY_DP_RATE_LIMIT", max_payload_size=8196, **kwargs):
        # Added for compatibility with old versions
        if kwargs.get('rate_limit') is not None or kwargs.get('dp_rate_limit') is not None:
            messages_rate_limit = messages_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', messages_rate_limit) # noqa
            telemetry_rate_limit = telemetry_rate_limit if kwargs.get('rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('rate_limit', telemetry_rate_limit) # noqa
            telemetry_dp_rate_limit = telemetry_dp_rate_limit if kwargs.get('dp_rate_limit') == "DEFAULT_RATE_LIMIT" else kwargs.get('dp_rate_limit', telemetry_dp_rate_limit) # noqa
        self._client = paho.Client(protocol=5, client_id=client_id, callback_api_version=CallbackAPIVersion.VERSION2)
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
        self.__error_logged = 0
        self.max_payload_size = max_payload_size
        self.service_configuration_callback = self.on_service_configuration
        telemetry_rate_limit, telemetry_dp_rate_limit = RateLimit.get_rate_limits_by_host(self.__host,
                                                                                          telemetry_rate_limit,
                                                                                          telemetry_dp_rate_limit)
        messages_rate_limit = RateLimit.get_rate_limit_by_host(self.__host, messages_rate_limit)

        self._messages_rate_limit = RateLimit(messages_rate_limit, "Rate limit for messages")
        self._telemetry_rate_limit = RateLimit(telemetry_rate_limit, "Rate limit for telemetry messages")
        self._telemetry_dp_rate_limit = RateLimit(telemetry_dp_rate_limit, "Rate limit for telemetry data points")
        self.max_inflight_messages_set(self._telemetry_rate_limit.get_minimal_limit())
        self.__attrs_request_timeout = {}
        self.__timeout_thread = Thread(target=self.__timeout_check, name="Timeout check thread")
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
        self.rate_limits_received = False
        self.__request_service_configuration_required = False
        self.__service_loop = Thread(target=self.__service_loop, name="Service loop", daemon=True)
        self.__service_loop.start()
        self.__messages_limit_reached_set_time = (0,0)
        self.__datapoints_limit_reached_set_time = (0,0)

    def __service_loop(self):
        while not self.stopped:
            if self.__request_service_configuration_required:
                self.request_service_configuration(self.service_configuration_callback)
                self.__request_service_configuration_required = False
            elif self.firmware_received:
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
            sleep(0.05)

    def _on_publish(self, client, userdata, mid, rc=None, properties=None):
        if isinstance(rc, ReasonCodes) and rc.value != 0:
            log.debug("Publish failed with result code %s (%s) ", str(rc.value), rc.getName())
            if rc.value in [151, 131]:
                if self.__messages_limit_reached_set_time[1] - monotonic() > self.__messages_limit_reached_set_time[0]:
                    self.__messages_limit_reached_set_time = self._messages_rate_limit.reach_limit()
                if self.__datapoints_limit_reached_set_time[1] - monotonic() > self.__datapoints_limit_reached_set_time[0]:
                    self._telemetry_dp_rate_limit.reach_limit()
        if rc.value == 0:
            if self.__messages_limit_reached_set_time[0] > 0 and self.__messages_limit_reached_set_time[1] > 0:
                self.__messages_limit_reached_set_time = (0, 0)
            if self.__datapoints_limit_reached_set_time[0] > 0 and self.__datapoints_limit_reached_set_time[1] > 0:
                self.__datapoints_limit_reached_set_time = (0, 0)

    def _on_disconnect(self, client: paho.Client, userdata, disconnect_flags, reason=None, properties=None):
        self.__is_connected = False
        with self._client._out_message_mutex:
            client._out_packet.clear()
            client._out_messages.clear()
        client._in_messages.clear()
        self.__attr_request_number = 0
        self.__device_max_sub_id = 0
        self.__device_client_rpc_number = 0
        self.__device_sub_dict = {}
        self.__device_client_rpc_dict = {}
        self.__attrs_request_timeout = {}
        result_code = reason.value
        if disconnect_flags.is_disconnect_packet_from_server:
            log.warning("MQTT client was disconnected by server with reason code %s (%s) ",
                     str(result_code), reason.getName())
        else:
            log.info("MQTT client was disconnected by client with reason code %s (%s) ",
                     str(result_code), reason.getName())
        log.debug("Client: %s, user data: %s, result code: %s. Description: %s",
                  str(client), str(userdata),
                  str(result_code), reason.getName())

    def _on_connect(self, client, userdata, connect_flags, result_code, properties, *extra_params):
        if result_code == 0:
            self.__is_connected = True
            log.info("MQTT client %r - Connected!", client)
            if properties:
                log.debug("MQTT client %r - CONACK Properties: %r", client, properties)
                config = {}
                if hasattr(properties, 'MaximumPacketSize'):
                    config['maxPayloadSize'] = int(properties.MaximumPacketSize * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
                if hasattr(properties, 'ReceiveMaximum'):
                    config['maxInflightMessages'] = properties.ReceiveMaximum
                if config:
                    self.on_service_configuration(None, config)
            self._subscribe_to_topic(ATTRIBUTES_TOPIC, qos=self.quality_of_service)
            self._subscribe_to_topic(ATTRIBUTES_TOPIC + "/response/+", qos=self.quality_of_service)
            self._subscribe_to_topic(RPC_REQUEST_TOPIC + '+', qos=self.quality_of_service)
            self._subscribe_to_topic(RPC_RESPONSE_TOPIC + '+', qos=self.quality_of_service)
            self.__request_service_configuration_required = True
        else:
            log.error("Connection failed with result code %s (%s) ",
                      str(result_code.value), result_code.getName())

        if callable(self.__connect_callback):
            sleep(.2)
            if "tb_client" in signature(self.__connect_callback).parameters:
                self.__connect_callback(client, userdata, connect_flags, result_code, properties, *extra_params, tb_client=self)
            else:
                self.__connect_callback(client, userdata, connect_flags, result_code, *extra_params)

        if result_code.value in [159, 151]:
            log.debug("Connection rate limit reached, waiting before reconnecting...")
            sleep(1) # Wait for 1 second before reconnecting, if connection rate limit is reached
            log.debug("Reconnecting allowed...")

    def get_firmware_update(self):
        self._client.subscribe("v2/fw/response/+")
        self.send_telemetry(self.current_firmware_info)
        self.__request_firmware_info()

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
            self._messages_rate_limit.increase_rate_limit_counter()
            request_id = message.topic[len(RPC_REQUEST_TOPIC):len(message.topic)]
            if self.__device_on_server_side_rpc_response:
                self.__device_on_server_side_rpc_response(request_id, content)
        elif message.topic.startswith(RPC_RESPONSE_TOPIC):
            self._messages_rate_limit.increase_rate_limit_counter()
            with self._lock:
                request_id = int(message.topic[len(RPC_RESPONSE_TOPIC):len(message.topic)])
                if self.__device_client_rpc_dict.get(request_id):
                    callback = self.__device_client_rpc_dict.pop(request_id)
                else:
                    callback = None
            if callback is not None:
                callback(request_id, content, None)
        elif message.topic == ATTRIBUTES_TOPIC:
            self._messages_rate_limit.increase_rate_limit_counter()
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
            self._messages_rate_limit.increase_rate_limit_counter()
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
        else:
            log.debug("Message received with topic: %s", message.topic)

        if message.topic.startswith("v1/devices/me/attributes"):
            self._messages_rate_limit.increase_rate_limit_counter()
            self.firmware_info = loads(message.payload)
            if "/response/" in message.topic:
                self.firmware_info = self.firmware_info.get("shared", {}) if isinstance(self.firmware_info, dict) else {} # noqa
            if ((self.firmware_info.get(FW_VERSION_ATTR) is not None
                and self.firmware_info.get(FW_VERSION_ATTR) != self.current_firmware_info.get("current_" + FW_VERSION_ATTR)) # noqa
                    or (self.firmware_info.get(FW_TITLE_ATTR) is not None
                        and self.firmware_info.get(FW_TITLE_ATTR) != self.current_firmware_info.get("current_" + FW_TITLE_ATTR))): # noqa
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
        self._client.publish(
            f"v2/fw/request/{self.__firmware_request_id}/chunk/{self.__current_chunk}",
            payload=payload, qos=1)

    def __on_firmware_received(self, version_to):
        with open(self.firmware_info.get(FW_TITLE_ATTR), "wb") as firmware_file:
            firmware_file.write(self.firmware_data)
        log.info('Firmware is updated!\n Current firmware version is: %s' % version_to)

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
        if inflight < 0:
            log.error("Inflight messages number must be equal or greater than 0")
            return
        self._client._max_inflight_messages = inflight

    def max_queued_messages_set(self, queue_size):
        """Set the maximum number of outgoing messages with QoS>0 that can be pending in the outgoing message queue.
        Defaults to 0. 0 means unlimited. When the queue is full, any further outgoing messages would be dropped."""
        if queue_size < 0:
            raise ValueError("Invalid queue size.")

        self._client._max_queued_messages = queue_size

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
        self._publish_data(payload, RPC_REQUEST_TOPIC + str(rpc_request_id), self.quality_of_service)

    def request_service_configuration(self, callback):
        self.send_rpc_call("getSessionLimits", {"timeout": 5000}, callback)

    def on_service_configuration(self, _, response, *args, **kwargs):
        global log
        log = logging.getLogger('tb_connection')
        if "error" in response:
            log.warning("Timeout while waiting for service configuration!, session will use default configuration.")
            self.rate_limits_received = True
            return
        service_config = response
        if not isinstance(service_config, dict) or 'rateLimits' not in service_config:
            log.warning("Cannot retrieve service configuration, session will use default configuration.")
            log.debug("Received the following response: %r", service_config)
            return
        if service_config.get("rateLimits"):
            rate_limits_config = service_config.get("rateLimits")

            if rate_limits_config.get('messages'):
                self._messages_rate_limit.set_limit(rate_limits_config.get('messages'))
            else:
                self._messages_rate_limit.set_limit('0:0,')

            if rate_limits_config.get('telemetryMessages'):
                self._telemetry_rate_limit.set_limit(rate_limits_config.get('telemetryMessages'))
            else:
                self._telemetry_rate_limit.set_limit('0:0,')

            if rate_limits_config.get('telemetryDataPoints'):
                self._telemetry_dp_rate_limit.set_limit(rate_limits_config.get('telemetryDataPoints'))
            else:
                self._telemetry_dp_rate_limit.set_limit('0:0,')

        if service_config.get('maxInflightMessages'):
            use_messages_rate_limit_factor = self._messages_rate_limit.has_limit()
            use_telemetry_rate_limit_factor = self._telemetry_rate_limit.has_limit()
            service_config_inflight_messages = int(service_config.get('maxInflightMessages', 100))
            if use_messages_rate_limit_factor and use_telemetry_rate_limit_factor:
                max_inflight_messages = int(min(self._messages_rate_limit.get_minimal_limit(),
                                                self._telemetry_rate_limit.get_minimal_limit(),
                                                service_config_inflight_messages) * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
            elif use_messages_rate_limit_factor:
                max_inflight_messages = int(min(self._messages_rate_limit.get_minimal_limit(),
                                                service_config_inflight_messages) * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
            elif use_telemetry_rate_limit_factor:
                max_inflight_messages = int(min(self._telemetry_rate_limit.get_minimal_limit(),
                                                service_config_inflight_messages) * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
            else:
                max_inflight_messages = int(service_config.get('maxInflightMessages', 100) * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
                if max_inflight_messages == 0:
                    max_inflight_messages = 10_000  # No limitation on device queue on transport level
            if max_inflight_messages < 1:
                max_inflight_messages = 1
            self.max_inflight_messages_set(max_inflight_messages)

            if (not self._messages_rate_limit.has_limit() and
                    not self._telemetry_rate_limit.has_limit() and
                    not self._telemetry_dp_rate_limit.has_limit() and
                    not kwargs.get("gateway_limits_present", False)):
                log.debug("No rate limits for device, setting max_queued_messages to 50000")
                self.max_queued_messages_set(50000)
            else:
                log.debug("Rate limits for device, setting max_queued_messages to %r", max_inflight_messages)
                self.max_queued_messages_set(max_inflight_messages)

        if service_config.get('maxPayloadSize'):
            self.max_payload_size = int(int(service_config.get('maxPayloadSize')) * DEFAULT_RATE_LIMIT_PERCENTAGE / 100)
        log.info("Service configuration was successfully retrieved and applied.")
        log.info("Current device limits: %r", service_config)
        self.rate_limits_received = True

    def set_server_side_rpc_request_handler(self, handler):
        """Set the callback that will be called when a server-side RPC is received."""
        self.__device_on_server_side_rpc_response = handler

    def _wait_for_rate_limit_released(self, timeout, message_rate_limit, dp_rate_limit=None, amount=1):
        if not message_rate_limit.has_limit() and not (dp_rate_limit is None or dp_rate_limit.has_limit()):
            return
        start_time = int(monotonic())
        dp_rate_limit_timeout = dp_rate_limit.get_minimal_timeout() if dp_rate_limit is not None else 0
        timeout = max(message_rate_limit.get_minimal_timeout(), dp_rate_limit_timeout, timeout) + 10
        timeout_updated = False
        disconnected = False
        limit_reached_check = True
        log_posted = False
        waited = False
        while limit_reached_check:

            message_rate_limit_check = message_rate_limit.check_limit_reached()
            datapoints_rate_limit_check = dp_rate_limit.check_limit_reached(amount=amount) if dp_rate_limit is not None else False
            limit_reached_check = (message_rate_limit_check
                                   or datapoints_rate_limit_check
                                   or not self.is_connected())
            if isinstance(limit_reached_check, tuple) and timeout < limit_reached_check[1]:
                timeout = limit_reached_check[1]
            if not timeout_updated and limit_reached_check:
                timeout += 10
                timeout_updated = True
            if self.stopped:
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if not disconnected and not self.is_connected():
                log.warning("Waiting for connection to be established before sending data to ThingsBoard!")
                disconnected = True
                timeout = max(timeout, 180) + 10
            if int(monotonic()) >= timeout + start_time:
                if message_rate_limit_check:
                    log.warning("Timeout while waiting for rate limit for messages to be released! Rate limit: %r:%r",
                                message_rate_limit_check,
                                message_rate_limit_check)
                elif datapoints_rate_limit_check:
                    log.warning("Timeout while waiting for rate limit for data points to be released! Rate limit: %r:%r",
                                datapoints_rate_limit_check,
                                datapoints_rate_limit_check)
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if not log_posted and limit_reached_check:
                if log.isEnabledFor(logging.DEBUG):
                    if isinstance(message_rate_limit_check, tuple):
                        log.debug("Rate limit for messages (%r messages per %r second(s)) - almost reached, waiting for rate limit to be released...",
                                  *message_rate_limit_check)
                    if isinstance(datapoints_rate_limit_check, tuple):
                        log.debug("Rate limit for data points (%r data points per %r second(s)) - almost reached, waiting for rate limit to be released...",
                                  *datapoints_rate_limit_check)
                waited = True
                log_posted = True
            if limit_reached_check:
                sleep(.005)
        if waited:
            log.debug("Rate limit released, sending data to ThingsBoard...")

    def _wait_until_current_queued_messages_processed(self):
        logger = None

        max_wait_time = 300
        log_interval = 5
        stuck_threshold = 15
        polling_interval = 0.05
        max_inflight = self._client._max_inflight_messages

        if len(self._client._out_messages) < max_inflight or max_inflight == 0:
            return

        waiting_start = monotonic()
        last_log_time = waiting_start
        last_queue_size = len(self._client._out_messages)
        last_queue_change_time = waiting_start

        while not self.stopped:
            now = monotonic()
            elapsed = now - waiting_start
            current_queue_size = len(self._client._out_messages)

            if current_queue_size < max_inflight:
                return

            if current_queue_size != last_queue_size:
                last_queue_size = current_queue_size
                last_queue_change_time = now

            if (now - last_queue_change_time > stuck_threshold
                    and not self._client.is_connected()):
                if logger is None:
                    logger = logging.getLogger('tb_connection')
                logger.warning(
                    "MQTT out_messages queue is stuck (%d messages) and client is disconnected. "
                    "Clearing queue after %.2f seconds.",
                    current_queue_size, now - last_queue_change_time
                )
                with self._client._out_message_mutex:
                    self._client._out_packet.clear()
                return

            if now - last_log_time >= log_interval:
                if logger is None:
                    logger = logging.getLogger('tb_connection')
                logger.debug(
                    "Waiting for MQTT queue to drain: %d messages (max inflight %d). "
                    "Elapsed: %.2f s",
                    current_queue_size, max_inflight, elapsed
                )
                last_log_time = now

            if elapsed > max_wait_time:
                if logger is None:
                    logger = logging.getLogger('tb_connection')
                logger.warning(
                    "MQTT wait timeout reached (%.2f s). Queue still has %d messages.",
                    elapsed, current_queue_size
                )
                return

            sleep(polling_interval)

    def _send_request(self, _type, kwargs, timeout=DEFAULT_TIMEOUT, device=None,
                      msg_rate_limit=None, dp_rate_limit=None):
        topic = kwargs['topic']
        if msg_rate_limit is None:
            if topic == TELEMETRY_TOPIC or topic ==ATTRIBUTES_TOPIC:
                msg_rate_limit = self._telemetry_rate_limit
            else:
                msg_rate_limit = self._messages_rate_limit
        if dp_rate_limit is None:
            if topic == TELEMETRY_TOPIC or topic ==ATTRIBUTES_TOPIC:
                dp_rate_limit = self._telemetry_dp_rate_limit
            else:
                dp_rate_limit = self.EMPTY_RATE_LIMIT
        if msg_rate_limit.has_limit() or dp_rate_limit.has_limit():
            msg_rate_limit.increase_rate_limit_counter()
            is_reached = self._wait_for_rate_limit_released(timeout, msg_rate_limit, dp_rate_limit)
            if is_reached:
                return is_reached

        if _type == TBSendMethod.PUBLISH:
            self.__add_metadata_to_data_dict_from_device(kwargs["payload"])
            return self.__send_publish_with_limitations(kwargs, timeout, device, msg_rate_limit, dp_rate_limit)
        elif _type == TBSendMethod.SUBSCRIBE:
            return self._client.subscribe(**kwargs)
        elif _type == TBSendMethod.UNSUBSCRIBE:
            return self._client.unsubscribe(**kwargs)

    def __add_metadata_to_data_dict_from_device(self, data):
        if isinstance(data, dict) and ("metadata" in data and isinstance(data["metadata"], dict)):
            data["metadata"]["publishedTs"] = int(timestamp() * 1000)
        elif isinstance(data, list):
            current_time = int(timestamp() * 1000)
            for data_item in data:
                if isinstance(data_item, dict):
                    if 'ts' in data_item and ('metadata' in data_item and isinstance(data_item["metadata"], dict)):
                        data_item["metadata"]["publishedTs"] = current_time
        elif isinstance(data, dict):
            for key, value in data.items():
                self.__add_metadata_to_data_dict_from_device(value)

    def __get_rate_limits_by_topic(self, topic, device=None, msg_rate_limit=None, dp_rate_limit=None):
        if device is not None:
            return msg_rate_limit, dp_rate_limit
        else:
            if topic == TELEMETRY_TOPIC:
                return self._telemetry_rate_limit, self._telemetry_dp_rate_limit
            else:
                return self._messages_rate_limit, None

    def __send_publish_with_limitations(self, kwargs, timeout, device=None, msg_rate_limit: RateLimit = None,
                                        dp_rate_limit: RateLimit = None):
        data = kwargs.get("payload")
        if isinstance(data, str):
            data = loads(data)
        topic = kwargs["topic"]
        attributes_format = topic.endswith('attributes')
        if topic.endswith('telemetry') or attributes_format:
            if device is None or data.get(device) is None:
                device_split_messages = self._split_message(data, int(dp_rate_limit.get_minimal_limit()), self.max_payload_size) # noqa
                if attributes_format:
                    split_messages = [{'message': msg_data, 'datapoints': len(msg_data)} for split_message in device_split_messages for msg_data in split_message['data']] # noqa
                else:
                    split_messages = [{'message': split_message['data'], 'datapoints': split_message['datapoints']} for split_message in device_split_messages] # noqa
            else:
                device_data = data.get(device)
                device_split_messages = self._split_message(device_data, int(dp_rate_limit.get_minimal_limit()), self.max_payload_size) # noqa
                if attributes_format:
                    split_messages = [{'message': {device: msg_data}, 'datapoints': len(msg_data)} for split_message in device_split_messages for msg_data in split_message['data']] # noqa
                else:
                    split_messages = [{'message': {device: split_message['data']}, 'datapoints': split_message['datapoints']} for split_message in device_split_messages] # noqa
        else:
            split_messages = [{'message': data, 'datapoints': self._count_datapoints_in_message(data, device)}]

        results = []
        for part in split_messages:
            if not part:
                continue
            self.__send_split_message(results, part, kwargs, timeout, device, msg_rate_limit, dp_rate_limit, topic)
        return TBPublishInfo(results)

    def __send_split_message(self, results, part, kwargs, timeout, device, msg_rate_limit, dp_rate_limit,
                             topic):
        if msg_rate_limit.has_limit() or dp_rate_limit.has_limit():
            dp_rate_limit.increase_rate_limit_counter(part['datapoints'])
            rate_limited = self._wait_for_rate_limit_released(timeout,
                                                              message_rate_limit=msg_rate_limit,
                                                              dp_rate_limit=dp_rate_limit,
                                                              amount=part['datapoints'])
            if rate_limited:
                return rate_limited
        if msg_rate_limit.has_limit() or dp_rate_limit.has_limit():
            msg_rate_limit.increase_rate_limit_counter()
        kwargs["payload"] = dumps(part['message'], option=OPT_NON_STR_KEYS)
        if msg_rate_limit.has_limit() or dp_rate_limit.has_limit():
            self._wait_until_current_queued_messages_processed()
        if not self.stopped:
            if device is not None:
                log.debug("Device: %s, Sending message to topic: %s ", device, topic)
            if msg_rate_limit.has_limit() or dp_rate_limit.has_limit():
                if part['datapoints'] > 0:
                    log.debug("Sending message with %i datapoints", part['datapoints'])
                    if log.isEnabledFor(5) and hasattr(log, 'trace'):
                        log.trace("Message payload: %r", kwargs["payload"])
                    log.debug("Rate limits after sending message: %r", msg_rate_limit.__dict__)
                    log.debug("Data points rate limits after sending message: %r", dp_rate_limit.__dict__)
                else:
                    if log.isEnabledFor(5) and hasattr(log, 'trace'):
                        log.trace("Sending message with %r", kwargs["payload"])
                    log.debug("Rate limits after sending message: %r", msg_rate_limit.__dict__)
                    log.debug("Data points rate limits after sending message: %r", dp_rate_limit.__dict__)
        result = self._client.publish(**kwargs)
        if result.rc == MQTT_ERR_QUEUE_SIZE:
            error_appear_counter = 1
            sleep_time = 0.1  # 100 ms, in case of change - change max tries in while loop
            while not self.stopped and result.rc == MQTT_ERR_QUEUE_SIZE:
                error_appear_counter += 1
                if error_appear_counter > 78:  # 78 tries ~ totally 300 seconds for sleep 0.1
                    log.warning("Cannot send message to platform in %i seconds, queue size exceeded, current max inflight messages: %r, max queued messages: %r.",  # noqa
                                int(error_appear_counter * sleep_time),
                                self._client._max_inflight_messages,
                                self._client._max_queued_messages)
                if int(monotonic()) - self.__error_logged > 10:
                    log.debug("Queue size exceeded, waiting for messages to be processed by paho client.")
                    self.__error_logged = int(monotonic())
                sleep(sleep_time)  # Give some time for paho to process messages
                result = self._client.publish(**kwargs)
        results.append(result)

    def _subscribe_to_topic(self, topic, qos=None, timeout=DEFAULT_TIMEOUT):
        if qos is None:
            qos = self.quality_of_service

        waiting_for_connection_message_time = 0
        while not self.is_connected() and not self.stopped:
            if self.stopped:
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if monotonic() - waiting_for_connection_message_time > 10.0:
                log.warning("Waiting for connection to be established before subscribing for data on ThingsBoard!")
                waiting_for_connection_message_time = monotonic()
            sleep(0.01)

        return self._send_request(TBSendMethod.SUBSCRIBE,
                                  {"topic": topic, "qos": qos},
                                  timeout,
                                  msg_rate_limit=self._messages_rate_limit)

    def _publish_data(self, data, topic, qos, timeout=DEFAULT_TIMEOUT, device=None,
                      msg_rate_limit=None, dp_rate_limit=None):
        if qos is None:
            qos = self.quality_of_service
        if qos not in (0, 1):
            log.exception("Quality of service (qos) value must be 0 or 1")
            raise TBQoSException("Quality of service (qos) value must be 0 or 1")

        waiting_for_connection_message_time = 0.0
        while not self.is_connected():
            if self.stopped:
                return TBPublishInfo(paho.MQTTMessageInfo(None))
            if monotonic() - waiting_for_connection_message_time > 10.0:
                log.warning("Waiting for connection to be established before sending data to ThingsBoard!")
                waiting_for_connection_message_time = monotonic()
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

        start_processing_attribute_request = int(monotonic())

        attr_request_number = self._add_attr_request_callback(callback)

        info = self._publish_data(msg, ATTRIBUTES_TOPIC_REQUEST + str(attr_request_number), self.quality_of_service)

        self.__attrs_request_timeout[attr_request_number] = start_processing_attribute_request + 20
        return info

    def _add_attr_request_callback(self, callback):
        with self._lock:
            self.__attr_request_number += 1
            self._attr_request_dict.update({self.__attr_request_number: callback})
            attr_request_number = self.__attr_request_number
        return attr_request_number

    def __timeout_check(self):
        while not self.stopped:
            current_time = int(monotonic())
            for (attr_request_number, ts) in tuple(self.__attrs_request_timeout.items()):
                if current_time < ts:
                    continue

                with self._lock:
                    callback = None
                    if self._attr_request_dict.get(attr_request_number):
                        callback = self._attr_request_dict.pop(attr_request_number)

                if callback is not None:
                    if isinstance(callback, tuple):
                        callback[0](None, TBTimeoutException("Timeout while waiting for a reply for attribute request from ThingsBoard!"), # noqa
                                    callback[1])
                    else:
                        callback(None, TBTimeoutException("Timeout while waiting for a reply for attribute request from ThingsBoard!")) # noqa

                self.__attrs_request_timeout.pop(attr_request_number)

            sleep(0.1)

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
            elif isinstance(data.get(device), dict):
                datapoints += TBDeviceMqttClient._count_datapoints_in_message(data.get(device, data.get('device')))
            else:
                datapoints += 1
        else:
            if isinstance(data, dict):
                datapoints += TBDeviceMqttClient._get_data_points_from_message(data)
            elif isinstance(data, list):
                for item in data:
                    datapoints += TBDeviceMqttClient._get_data_points_from_message(item)
            else:
                datapoints += 1
        return datapoints

    @staticmethod
    def _get_data_points_from_message(data):
        if isinstance(data, dict):
            if data.get("ts") is not None and data.get("values") is not None:
                datapoints_in_message_amount = len(data['values']) + len(str(data['values'])) / 1000
            else:
                datapoints_in_message_amount = len(data.keys()) + len(str(data)) / 1000
        else:
            datapoints_in_message_amount = len(data) + len(str(data)) / 1000
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
    def _split_message(message_pack, datapoints_max_count, max_payload_size):
        if not message_pack:
            return []

        split_messages = []

        if isinstance(message_pack, dict) and message_pack.get('device') and len(message_pack) in [1, 2]:
            return [{
                'data': message_pack,
                'datapoints': TBDeviceMqttClient._count_datapoints_in_message(message_pack),
                'message': message_pack
            }]

        if not isinstance(message_pack, list):
            message_pack = [message_pack]

        def _get_metadata_repr(metadata):
            if isinstance(metadata, dict):
                return tuple(sorted(metadata.items()))
            return None

        def estimate_chunk_size(chunk):
            if isinstance(chunk, dict) and "values" in chunk:
                size = sum(len(str(k)) + len(str(v)) for k, v in chunk["values"].items())
                size += len(str(chunk.get("ts", "")))
                if "metadata" in chunk:
                    size += sum(len(str(k)) + len(str(v)) for k, v in chunk["metadata"].items())
                return size + 40
            elif isinstance(chunk, dict):
                return sum(len(str(k)) + len(str(v)) for k, v in chunk.items()) + 20
            else:
                return len(str(chunk)) + 20


        ts_group_cache = {}
        current_message = {"data": [], "datapoints": 0}
        current_datapoints = 0
        current_size = 0

        def flush_current_message():
            nonlocal current_message, current_datapoints, current_size
            if current_message["data"]:
                split_messages.append(current_message)
            current_message = {"data": [], "datapoints": 0}
            current_datapoints = 0
            current_size = 0

        def split_and_add_chunk(chunk, chunk_datapoints):
            nonlocal current_message, current_datapoints, current_size
            chunk_size = estimate_chunk_size(chunk)

            if (datapoints_max_count > 0 and current_datapoints + chunk_datapoints > datapoints_max_count) or \
                    (current_size + chunk_size > max_payload_size):
                flush_current_message()

            if chunk_datapoints > datapoints_max_count > 0 or chunk_size > max_payload_size:
                keys = list(chunk["values"].keys()) if "values" in chunk else list(chunk.keys())
                if len(keys) == 1:
                    current_message["data"].append(chunk)
                    current_message["datapoints"] += chunk_datapoints
                    current_size += chunk_size
                    return

                max_step = int(datapoints_max_count) if datapoints_max_count > 0 else len(keys)
                if max_step < 1:
                    max_step = 1

                for i in range(0, len(keys), max_step):
                    sub_values = (
                        {k: chunk["values"][k] for k in keys[i:i + max_step]}
                        if "values" in chunk else
                        {k: chunk[k] for k in keys[i:i + max_step]}
                    )

                    if "ts" in chunk:
                        sub_chunk = {"ts": chunk["ts"], "values": sub_values}
                        if "metadata" in chunk:
                            sub_chunk["metadata"] = chunk["metadata"]
                    else:
                        sub_chunk = sub_values.copy()

                    sub_datapoints = len(sub_values)
                    sub_size = estimate_chunk_size(sub_chunk)

                    if sub_size > max_payload_size:
                        current_message["data"].append(sub_chunk)
                        current_message["datapoints"] += sub_datapoints
                        current_size += sub_size
                        continue

                    split_and_add_chunk(sub_chunk, sub_datapoints)
                return

            current_message["data"].append(chunk)
            current_message["datapoints"] += chunk_datapoints
            current_size += chunk_size

        def add_chunk_to_current_message(chunk, chunk_datapoints):
            nonlocal current_message, current_datapoints, current_size
            chunk_size = estimate_chunk_size(chunk)

            if (datapoints_max_count > 0 and chunk_datapoints > datapoints_max_count) or chunk_size > max_payload_size:
                split_and_add_chunk(chunk, chunk_datapoints)
                return

            if (datapoints_max_count > 0 and current_datapoints + chunk_datapoints > datapoints_max_count) or \
                    (current_size + chunk_size > max_payload_size):
                flush_current_message()

            current_message["data"].append(chunk)
            current_message["datapoints"] += chunk_datapoints
            current_size += chunk_size

            if datapoints_max_count > 0 and current_message["datapoints"] == datapoints_max_count:
                flush_current_message()

        def flush_ts_group(ts_key, ts, metadata_repr):
            if ts_key not in ts_group_cache:
                return
            values, _, metadata = ts_group_cache.pop(ts_key)
            keys = list(values.keys())
            step = int(datapoints_max_count) if datapoints_max_count > 0 else len(keys)
            if step < 1:
                step = 1
            for i in range(0, len(keys), step):
                chunk_values = {k: values[k] for k in keys[i:i + step]}
                if ts is not None:
                    chunk = {"ts": ts, "values": chunk_values}
                    if metadata:
                        chunk["metadata"] = metadata
                else:
                    chunk = chunk_values.copy()
                add_chunk_to_current_message(chunk, len(chunk_values))

        for message in message_pack:
            if not isinstance(message, dict):
                continue

            ts = message.get("ts", None)
            metadata = message.get("metadata") if isinstance(message.get("metadata"), dict) else None
            values = message.get("values") if isinstance(message.get("values"), dict) else \
                message if isinstance(message, dict) else {}

            metadata_repr = _get_metadata_repr(metadata)
            ts_key = (ts, metadata_repr)

            for key, value in values.items():
                pair_size = len(str(key)) + len(str(value)) + 4
                if ts_key not in ts_group_cache:
                    ts_group_cache[ts_key] = ({}, 0, metadata)

                group_values, group_size, group_metadata = ts_group_cache[ts_key]

                can_add = (
                        (datapoints_max_count == 0 or len(group_values) < datapoints_max_count) and
                        (group_size + pair_size <= max_payload_size)
                )

                if can_add:
                    group_values[key] = value
                    ts_group_cache[ts_key] = (group_values, group_size + pair_size, group_metadata)
                else:
                    flush_ts_group(ts_key, ts, metadata_repr)
                    ts_group_cache[ts_key] = ({key: value}, pair_size, metadata)

        for ts_key in list(ts_group_cache.keys()):
            ts, metadata_repr = ts_key
            flush_ts_group(ts_key, ts, metadata_repr)

        flush_current_message()
        return split_messages

    @staticmethod
    def _datapoints_limit_reached(datapoints_max_count, current_datapoints_size, current_size):
        return 0 < datapoints_max_count <= current_datapoints_size + current_size // 1024

    @staticmethod
    def _payload_size_limit_reached(max_payload_size, current_size, additional_size):
        return current_size + additional_size >= max_payload_size

    def add_attrs_request_timeout(self, attr_request_number, timeout):
        self.__attrs_request_timeout[attr_request_number] = timeout


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
            provision_request = dumps(self.__provision_request, option=OPT_NON_STR_KEYS)
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
