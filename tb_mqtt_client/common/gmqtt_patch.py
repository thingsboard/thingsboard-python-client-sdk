#  Copyright 2025 ThingsBoard
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import asyncio
import heapq
import struct
from collections import defaultdict
from typing import Callable, Tuple, Optional

from gmqtt import Client
from gmqtt.mqtt.constants import MQTTCommands, MQTTv50, MQTTv311
from gmqtt.mqtt.handler import MqttPackageHandler, MQTTConnectError
from gmqtt.mqtt.package import PackageFactory
from gmqtt.mqtt.property import Property
from gmqtt.mqtt.protocol import BaseMQTTProtocol, MQTTProtocol
from gmqtt.mqtt.utils import unpack_variable_byte_integer, pack_variable_byte_integer

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage

logger = get_logger(__name__)


class PublishPacket(PackageFactory):
    @classmethod
    def build_package(cls, message: MqttPublishMessage, protocol, mid: int = None) -> Tuple[int, bytes]:
        dup_flag = 1 if message.dup else 0

        command = MQTTCommands.PUBLISH | (dup_flag << 3) | (message.qos << 1) | (message.retain & 0x1)

        packet = bytearray()
        packet.append(command)

        remaining_length = 2 + len(message.topic) + message.payload_size
        prop_bytes = cls._build_properties_data(message.properties, protocol_version=protocol.proto_ver)
        remaining_length += len(prop_bytes)

        if message.payload_size == 0:
            logger.debug("Sending PUBLISH (q%d), '%s' (NULL payload)", message.qos, message.topic)
        else:
            logger.debug("Sending PUBLISH (q%d), '%s', ... (%d bytes)", message.qos, message.topic, message.payload_size)

        if message.qos > 0:
            remaining_length += 2

        packet.extend(pack_variable_byte_integer(remaining_length))
        cls._pack_str16(packet, message.topic)

        if message.qos > 0:
            if mid is None:
                mid = cls.id_generator.next_id()
            packet.extend(struct.pack("!H", mid))

        packet.extend(prop_bytes)
        packet.extend(message.payload)

        return mid, packet


class PatchUtils:
    DISCONNECT_REASON_CODES = {
        0: "Normal disconnection",
        4: "Disconnect with Will Message",
        128: "Unspecified error",
        129: "Malformed Packet",
        130: "Protocol Error",
        131: "Implementation specific error",
        132: "Not authorized",
        133: "Server busy",
        134: "Bad credentials",
        135: "Keep Alive timeout",
        136: "Session taken over",
        137: "Topic Filter invalid",
        138: "Topic Name invalid",
        139: "Receive Maximum exceeded",
        140: "Topic Alias invalid",
        141: "Packet too large",
        142: "Session taken over",
        143: "Quota exceeded",
        144: "Administrative action",
        145: "Payload format invalid",
        146: "Retain not supported",
        147: "QoS not supported",
        148: "Use another server",
        149: "Server moved",
        150: "Shared Subscriptions not supported",
        151: "Connection rate exceeded",
        152: "Administrative action",
        153: "Subscription Identifiers not supported",
        154: "Wildcard Subscriptions not supported"
    }

    def __init__(self, client: Optional[Client], stop_event: asyncio.Event, retry_interval: int = 15):
        """
        Initialize PatchUtils with a client and retry interval.

        :param client: The MQTT client instance to patch.
        :param retry_interval: Interval in seconds to retry connection.
        """
        self.client = client
        self.retry_interval = retry_interval
        self._stop_event = stop_event
        self._retry_task = None

    @staticmethod
    def parse_mqtt_properties(packet: bytes) -> dict:
        """
        Parse MQTT 5.0 properties from a packet.
        """
        properties_dict = defaultdict(list)

        try:
            properties_len, _ = unpack_variable_byte_integer(packet)
            props = packet[:properties_len]

            while props:
                property_identifier = props[0]
                property_obj = Property.factory(id_=property_identifier)
                if property_obj is None:
                    logger.warning(f"Unknown property id={property_identifier}")
                    break

                result, props = property_obj.loads(props[1:])
                for k, v in result.items():
                    properties_dict[k].append(v)

        except Exception as e:
            logger.warning("Failed to parse properties: %s", e)

        return dict(properties_dict)


    @staticmethod
    def extract_reason_code(packet):
        """
        Extract the reason code from a disconnect packet.

        :param packet: The disconnect packet, which can be an object with a reason_code attribute or raw bytes
        :return: The reason code if found, None otherwise
        """
        reason_code = None
        if packet:
            if hasattr(packet, 'reason_code'):
                reason_code = packet.reason_code
            elif isinstance(packet, bytes) and len(packet) >= 2:
                reason_code = packet[1]

        return reason_code

    def patch_mqtt_handler_disconnect(patch_utils_instance):
        """
        Monkey-patch gmqtt.mqtt.handler.MqttPackageHandler._handle_disconnect_packet to properly
        handle server-initiated disconnect messages.
        """
        try:
            # Store the original method
            original_handle_disconnect = MqttPackageHandler._handle_disconnect_packet  # noqa

            # Define the patched method
            def patched_handle_disconnect_packet(self, cmd, packet):
                # Extract reason code
                reason_code = 0
                if packet and len(packet) >= 1:
                    reason_code = packet[0]

                # Parse properties if available
                properties = {}
                if packet and len(packet) > 1:
                    try:
                        properties = PatchUtils.parse_mqtt_properties(packet[1:])
                    except Exception as exc:
                        logger.warning("Failed to parse properties from disconnect packet: %s", exc)

                reason_desc = PatchUtils.DISCONNECT_REASON_CODES.get(reason_code, "Unknown reason")
                logger.trace("Server initiated disconnect with reason code: %s (%s)", reason_code, reason_desc)

                # Call the original method to handle reconnection
                # But don't call the on_disconnect callback, as we'll do that ourselves
                # with the extracted reason_code and properties
                self._clear_topics_aliases()
                future = asyncio.ensure_future(self.reconnect(delay=True))
                future.add_done_callback(self._handle_exception_in_future)

                # Call the on_disconnect callback with the client, reason_code, properties, and None for exc
                # since this is a server-initiated disconnect
                self.on_disconnect(self, reason_code, properties, None)

                # Set a flag on the connection object to indicate that on_disconnect has been called
                self._connection._on_disconnect_called = True
                original_handle_disconnect(self, cmd, packet)

            # Apply the patch
            MqttPackageHandler._handle_disconnect_packet = patched_handle_disconnect_packet
            logger.debug("Successfully patched gmqtt.mqtt.handler.MqttPackageHandler._handle_disconnect_packet")
            return True
        except (ImportError, AttributeError) as e:
            logger.warning("Failed to patch gmqtt handler: %s", e)
            return False

    def patch_handle_connack(patch_utils_instance):
        """
        Fully replaces gmqtt's _handle_connack_packet implementation, skipping internal QoS1 resend behavior
        and invoking a custom callback instead of calling the original method.
        """
        try:
            def new_handle_connack_packet(self, cmd, packet):
                try:
                    self._connected.set()

                    (session_present, result_code) = struct.unpack("!BB", packet[:2])

                    if result_code != 0:
                        self._logger.warning('[CONNACK] %s', hex(result_code))
                        self.failed_connections += 1
                        if result_code == 1 and self.protocol_version == MQTTv50:
                            self._logger.info('[CONNACK] Downgrading to MQTT 3.1 protocol version')
                            MQTTProtocol.proto_ver = MQTTv311
                            future = asyncio.ensure_future(self.reconnect(delay=True))
                            future.add_done_callback(self._handle_exception_in_future)
                            return
                        else:
                            self._error = MQTTConnectError(result_code)
                            asyncio.ensure_future(self.reconnect(delay=True))
                            return
                    else:
                        self.failed_connections = 0

                    if len(packet) > 2:
                        properties, _ = self._parse_properties(packet[2:])
                        if properties is None:
                            self._error = MQTTConnectError(10)
                            asyncio.ensure_future(self.disconnect())
                        self._connack_properties = properties
                        self._update_keepalive_if_needed()
                    else:
                        properties = {}

                    self._logger.debug('[CONNACK] session_present: %s, result: %s', hex(session_present),
                                       hex(result_code))

                    self.on_connect(self, session_present, result_code, self.properties)

                except Exception as e:
                    logger.exception("Error while handling CONNACK packet: %s", e)

            MqttPackageHandler._handle_connack_packet = new_handle_connack_packet
            logger.debug("Successfully patched gmqtt.mqtt.handler._handle_connack_packet (full replacement)")
            return True
        except (ImportError, AttributeError) as e:
            logger.warning("Failed to patch gmqtt handler: %s", e)
            return False

    def patch_gmqtt_protocol_connection_lost(patch_utils_instance):
        """
        Monkey-patch gmqtt.mqtt.protocol.BaseMQTTProtocol.connection_lost to suppress the
        default "[CONN CLOSE NORMALLY]" log message, as we handle disconnect logging in our code.

        Also, patch MQTTProtocol.connection_lost to include the reason code in the DISCONNECT package
        and pass the exception to the handler.
        """
        try:
            original_base_connection_lost = BaseMQTTProtocol.connection_lost
            def patched_base_connection_lost(self, exc):
                self._connected.clear()
                super(BaseMQTTProtocol, self).connection_lost(exc)
            BaseMQTTProtocol.connection_lost = patched_base_connection_lost

            original_mqtt_connection_lost = MQTTProtocol.connection_lost
            def patched_mqtt_connection_lost(self, exc):
                super(MQTTProtocol, self).connection_lost(exc)
                reason_code = 0
                properties = {}

                if exc:
                    if isinstance(exc, ConnectionRefusedError):
                        reason_code = 135  # Keep Alive timeout
                    elif isinstance(exc, TimeoutError):
                        reason_code = 135  # Keep Alive timeout
                    elif isinstance(exc, ConnectionResetError):
                        reason_code = 139  # Receive Maximum exceeded
                    elif isinstance(exc, ConnectionAbortedError):
                        reason_code = 136  # Session taken over
                    elif isinstance(exc, PermissionError):
                        reason_code = 132  # Not authorized
                    elif isinstance(exc, OSError):
                        reason_code = 130  # Protocol Error
                    else:
                        reason_code = 131  # Implementation specific error

                    if hasattr(exc, 'args') and exc.args:
                        properties['reason_string'] = [str(exc.args[0])]

                payload = struct.pack('!B', reason_code)

                self._connection._disconnect_exc = exc
                self._connection._disconnect_properties = properties

                self._connection.put_package((MQTTCommands.DISCONNECT, payload))

                if self._read_loop_future is not None:
                    self._read_loop_future.cancel()
                    self._read_loop_future = None

                self._queue = asyncio.Queue()

            MQTTProtocol.connection_lost = patched_mqtt_connection_lost

            original_call = MqttPackageHandler.__call__
            def patched_call(self, cmd, packet):
                try:
                    if cmd == MQTTCommands.DISCONNECT and hasattr(self._connection, '_disconnect_exc'):
                        reason_code = 0
                        if packet and len(packet) >= 1:
                            reason_code = packet[0]

                        properties = getattr(self._connection, '_disconnect_properties', {})
                        exc = getattr(self._connection, '_disconnect_exc', None)

                        if (not hasattr(self._connection, '_on_disconnect_called')
                                or not self._connection._on_disconnect_called):  # noqa
                            self._clear_topics_aliases()
                            future = asyncio.ensure_future(self.reconnect(delay=True))
                            future.add_done_callback(self._handle_exception_in_future)
                            self.on_disconnect(self, reason_code, properties, exc)
                        return None

                    return original_call(self, cmd, packet)
                except Exception as exception:
                    logger.error('[ERROR HANDLE PKG]', exc_info=exception)
                    return None

            MqttPackageHandler.__call__ = patched_call

            logger.debug("Successfully patched gmqtt.mqtt.protocol connection_lost methods")
            return True
        except (ImportError, AttributeError) as e:
            logger.warning("Failed to patch gmqtt protocol: %s", e)
            return False

    def patch_puback_handling(self, on_puback_with_reason_and_properties: Callable[[int, int, dict], None]):
        original_handler = MqttPackageHandler._handle_puback_packet
        def wrapped_handle_puback(self, cmd, packet):
            try:
                mid = struct.unpack("!H", packet[:2])[0]
                reason_code = 0
                properties = {}
                if len(packet) > 2:
                    reason_code = packet[2]
                    if len(packet) > 3:
                        props_payload = packet[3:]
                        properties = PatchUtils.parse_mqtt_properties(props_payload)
                logger.trace("PUBACK received for mid=%r, reason=%r", mid, reason_code)
                if hasattr(self._connection, 'persistent_storage'):
                    self._connection.persistent_storage.remove(mid)
                on_puback_with_reason_and_properties(mid, reason_code, properties)
            except Exception as e:
                logger.exception("Error while handling PUBACK with properties: %s", e)
            return original_handler(self, cmd, packet)
        MqttPackageHandler._handle_puback_packet = wrapped_handle_puback
        logger.debug("Patched _handle_puback_packet for QoS1 support.")

    def patch_storage(patch_utils_instance):
        async def pop_message_with_tm():
            (tm, mid, raw_package) = heapq.heappop(patch_utils_instance.client._persistent_storage._queue)

            patch_utils_instance.client._persistent_storage._check_empty()
            return tm, mid, raw_package
        patch_utils_instance.client._persistent_storage.pop_message = pop_message_with_tm

    async def _retry_loop(self):
        logger.debug("QoS1 retry loop started.")
        self.patch_storage()
        try:
            while not self._stop_event.is_set():
                current_tm = asyncio.get_event_loop().time()

                for _ in range(100):
                    if self._stop_event.is_set():
                        break

                    try:
                        msg = await asyncio.wait_for(self.client._persistent_storage.pop_message(), timeout=0.1)
                    except asyncio.TimeoutError:
                        break
                    except IndexError:
                        break
                    except Exception as e:
                        logger.warning("Error popping message: %s", e)
                        break

                    if msg is None:
                        break

                    tm, mid, mqtt_msg = msg

                    if current_tm - tm > self.retry_interval and self.client.is_connected:
                        mqtt_msg.dup = True
                        logger.error("Resending PUBLISH message with mid=%r, topic=%s", mid, mqtt_msg.topic)

                        try:
                            await self.client.put_retry_message(mqtt_msg)  # noqa This method sets in message service to the client
                        except AttributeError as e:
                            logger.trace("Failed to resend message with mid=%r: %s", mid, e)

                await asyncio.sleep(self.retry_interval)
        except asyncio.CancelledError:
            logger.debug("Retry loop cancelled.")
        except Exception as e:
            logger.exception("Unexpected error in retry loop: %s", e)
        finally:
            logger.debug("QoS1 retry loop stopped.")

    def start_retry_task(self):
        if not self._stop_event.is_set() and not self._retry_task:
            self._retry_task = asyncio.create_task(self._retry_loop())
            logger.debug("Retry task started.")

    async def stop_retry_task(self):
        if self._retry_task:
            self._stop_event.set()
            try:
                await asyncio.wait_for(self._retry_task, timeout=2)
            except asyncio.TimeoutError:
                logger.debug("Retry task did not finish in time, cancelling...")
                self._retry_task.cancel()
                try:
                    await self._retry_task
                except asyncio.CancelledError:
                    logger.debug("Retry task cancelled.")
            self._retry_task = None
        self._stop_event.clear()

    def apply(self, on_puback_with_reason_and_properties: Callable[[int, int, dict], None]):
        self.patch_puback_handling(on_puback_with_reason_and_properties)
        self.start_retry_task()
