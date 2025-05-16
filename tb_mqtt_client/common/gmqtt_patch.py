#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#


import struct
from types import MethodType
from typing import Callable
from collections import defaultdict

from tb_mqtt_client.common.logging_utils import get_logger
from gmqtt.mqtt.property import Property
from gmqtt.mqtt.utils import unpack_variable_byte_integer

logger = get_logger(__name__)


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
        Parse MQTT 5.0 properties from packet.
        """
        properties_dict = defaultdict(list)

        try:
            properties_len, packet = unpack_variable_byte_integer(packet)
            props = packet[:properties_len]
            packet = packet[properties_len:]

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

    client._handle_puback_packet = MethodType(wrapped_handle_puback, client)
