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
import struct
from collections import defaultdict
from typing import Callable

from gmqtt.mqtt.constants import MQTTCommands
from gmqtt.mqtt.handler import MqttPackageHandler
from gmqtt.mqtt.property import Property
from gmqtt.mqtt.protocol import BaseMQTTProtocol, MQTTProtocol
from gmqtt.mqtt.utils import unpack_variable_byte_integer

from tb_mqtt_client.common.logging_utils import get_logger

logger = get_logger(__name__)

# MQTT 5.0 Disconnect Reason Codes
DISCONNECT_REASON_CODES = {
    0: "Normal disconnection",
    4: "Disconnect with Will Message",
    128: "Unspecified error",
    129: "Malformed Packet",
    130: "Protocol Error",
    131: "Implementation specific error",
    132: "Not authorized",
    133: "Server busy",
    134: "Server shutting down",
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
    152: "Maximum connect time",
    153: "Subscription Identifiers not supported",
    154: "Wildcard Subscriptions not supported"
}


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

def patch_mqtt_handler_disconnect():
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
                    properties, _ = self._parse_properties(packet[1:])
                except Exception as exc:
                    logger.warning("Failed to parse properties from disconnect packet: %s", exc)

            reason_desc = DISCONNECT_REASON_CODES.get(reason_code, "Unknown reason")
            logger.debug("Server initiated disconnect with reason code: %s (%s)", reason_code, reason_desc)

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

def patch_gmqtt_protocol_connection_lost():
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
                # Determine reason code based on an exception type
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

                # Add an exception message to properties if available
                if hasattr(exc, 'args') and exc.args:
                    properties['reason_string'] = [str(exc.args[0])]

            # Pack the reason code into a payload
            payload = struct.pack('!B', reason_code)

            # Store the exception and properties in the connection object
            # so they can be accessed by the handler
            self._connection._disconnect_exc = exc
            self._connection._disconnect_properties = properties

            # Put the DISCONNECT package into the connection's package queue
            self._connection.put_package((MQTTCommands.DISCONNECT, payload))

            if self._read_loop_future is not None:
                self._read_loop_future.cancel()
                self._read_loop_future = None

            self._queue = asyncio.Queue()

        MQTTProtocol.connection_lost = patched_mqtt_connection_lost

        # Also patch MqttPackageHandler.__call__ to pass the exception and properties to on_disconnect
        original_call = MqttPackageHandler.__call__
        def patched_call(self, cmd, packet):
            try:
                if cmd == MQTTCommands.DISCONNECT and hasattr(self._connection, '_disconnect_exc'):
                    # This is a disconnect packet from connection_lost
                    # Extract reason code
                    reason_code = 0
                    if packet and len(packet) >= 1:
                        reason_code = packet[0]

                    # Get properties and exception from connection
                    properties = getattr(self._connection, '_disconnect_properties', {})
                    exc = getattr(self._connection, '_disconnect_exc', None)

                    # Check if on_disconnect has already been called
                    if (not hasattr(self._connection, '_on_disconnect_called')
                            or not self._connection._on_disconnect_called):  # noqa
                        # Call on_disconnect with the extracted values
                        self._clear_topics_aliases()
                        future = asyncio.ensure_future(self.reconnect(delay=True))
                        future.add_done_callback(self._handle_exception_in_future)
                        self.on_disconnect(self, reason_code, properties, exc)
                    return None

                # For other commands, call the original method
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


def patch_gmqtt_puback(client, on_puback_with_reason_and_properties: Callable[[int, int, dict], None]):
    """
    Monkey-patch gmqtt.Client instance to intercept PUBACK reason codes and properties.

    :param client: GMQTTClient instance
    :param on_puback_with_reason_and_properties: Callback with (mid, reason_code, properties_dict)
    """
    # Backup original method from MqttPackageHandler
    base_method = client.__class__.__bases__[0].__dict__.get('_handle_puback_packet')

    if base_method is None:
        logger.error("Could not find _handle_puback_packet in base class.")
        return

    def _parse_properties(packet: bytes) -> dict:
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
                    logger.warning(f"Unknown PUBACK property id={property_identifier}")
                    break

                result, props = property_obj.loads(props[1:])
                for k, v in result.items():
                    properties_dict[k].append(v)

        except Exception as e:
            logger.warning("Failed to parse PUBACK properties: %s", e)

        return dict(properties_dict)

    def wrapped_handle_puback(self, cmd, packet):
        try:
            mid = struct.unpack("!H", packet[:2])[0]
            reason_code = 0
            properties = {}

            if len(packet) > 2:
                reason_code = packet[2]
                if len(packet) > 3:
                    props_payload = packet[3:]
                    properties = _parse_properties(props_payload)

            on_puback_with_reason_and_properties(mid, reason_code, properties)
        except Exception as e:
            logger.exception("Error while handling PUBACK with properties: %s", e)

        return base_method(self, cmd, packet)

    MqttPackageHandler._handle_puback_packet = wrapped_handle_puback
