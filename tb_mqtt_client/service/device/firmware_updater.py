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

from random import randint
from zlib import crc32
from hashlib import sha256, sha384, sha512, md5
from subprocess import CalledProcessError
from asyncio import sleep
from os.path import sep
from typing import Awaitable, Callable, Optional
from tb_mqtt_client.common.install_package_utils import install_package
from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.constants import mqtt_topics
from tb_mqtt_client.constants.firmware import (
    FW_CHECKSUM_ALG_ATTR,
    FW_CHECKSUM_ATTR,
    FW_SIZE_ATTR,
    FW_STATE_ATTR,
    FW_TITLE_ATTR,
    FW_VERSION_ATTR,
    REQUIRED_SHARED_KEYS,
    FirmwareStates
)

from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry

try:
    from mmh3 import hash, hash128
except ImportError:
    try:
        from pymmh3 import hash, hash128
    except ImportError:
        try:
            install_package('mmh3')
        except CalledProcessError:
            install_package('pymmh3')

try:
    from mmh3 import hash, hash128 # noqa
except ImportError:
    from pymmh3 import hash, hash128

logger = get_logger(__name__)


class FirmwareUpdater:
    def __init__(self, client):
        self._log = logger
        self._client = client
        self._client._mqtt_manager.register_handler(mqtt_topics.DEVICE_FIRMWARE_UPDATE_RESPONSE_TOPIC,
                                                    self._handle_firmware_update)
        self._on_received_callback = None
        self._save_firmware = True
        self._save_path = './'
        self._firmware_request_id = 0
        self._chunk_size = 0
        self._current_chunk = 0
        self._firmware_data = b''
        self._target_firmware_length = 0
        self._target_checksum = 0
        self._target_checksum_alg = None
        self._target_version = None
        self._target_title = None
        self.current_firmware_info = {
            'current_' + FW_TITLE_ATTR: 'Initial',
            'current_' + FW_VERSION_ATTR: 'v0',
            FW_STATE_ATTR: FirmwareStates.IDLE.value
        }

    async def _handle_firmware_update(self, _, payload: bytes):
        self._firmware_data = self._firmware_data + payload
        self._current_chunk = self._current_chunk + 1

        self._log.debug('Getting chunk with number: %s. Chunk size is : %r byte(s).' % (
                self._current_chunk, self._chunk_size))

        if len(self._firmware_data) == self._target_firmware_length:
            self._log.info('Firmware download completed. '
                           'Total firmware size: %s byte(s).' % self._target_firmware_length)
            await self._verify_downloaded_firmware()
        else:
            await self._get_next_chunk()

    async def _get_next_chunk(self):
        if not self._chunk_size or self._chunk_size > self._target_firmware_length:
            payload = b''
        else:
            payload = str(self._chunk_size).encode()

        topic = mqtt_topics.build_firmware_update_request_topic(self._firmware_request_id, self._current_chunk)
        await self._client._message_queue.publish(topic=topic, payload=payload, datapoints_count=0, qos=1)

    async def _verify_downloaded_firmware(self):
        self._log.info('Verifying downloaded firmware...')

        self.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.DOWNLOADED.value
        await self._send_current_firmware_info()

        verified = self.verify_checksum(self._firmware_data,
                                        self._target_checksum,
                                        self._target_checksum_alg)

        if verified:
            self._log.debug('Checksum verified.')
            self.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.VERIFIED.value
        else:
            self._log.error('Checksum verification failed.')
            self.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.FAILED.value

        await self._send_current_firmware_info()

        if self.current_firmware_info[FW_STATE_ATTR] == FirmwareStates.VERIFIED.value:
            await self._apply_downloaded_firmware()

    async def _apply_downloaded_firmware(self):
        self._log.info('Applying downloaded firmware...')

        self.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.UPDATING.value
        await self._send_current_firmware_info()

        try:
            if self._save_firmware:
                self._save()
        except Exception as e:
            self._log.error('Failed to save firmware: %s', e)
            self.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.FAILED.value
            await self._send_current_firmware_info()
            return

        self.current_firmware_info = {
            "current_" + FW_TITLE_ATTR: self._target_title,
            "current_" + FW_VERSION_ATTR: self._target_version,
            FW_STATE_ATTR: FirmwareStates.UPDATED.value
        }

        await self._send_current_firmware_info()

        if self._on_received_callback:
            await self._on_received_callback(self._firmware_data, self.current_firmware_info)
        await self._client._mqtt_manager.unsubscribe(mqtt_topics.DEVICE_FIRMWARE_UPDATE_RESPONSE_TOPIC)

        self._log.info('Firmware is updated.')
        self._log.info('Current firmware version is: %s' % self._target_version)

    def _save(self):
        firmware_path = self._save_path + sep + self._target_title
        with open(firmware_path, "wb") as firmware_file:
            firmware_file.write(self._firmware_data)

    async def update(self, on_received_callback: Optional[Callable[[str], Awaitable[None]]] = None,
                     save_firmware: bool = True, firmware_save_path: Optional[str] = None):
        if not self._client._mqtt_manager.is_connected():
            self._log.error("Client is not connected. Cannot start firmware update.")
            return

        self._log.info("Starting firmware update process...")

        self._on_received_callback = on_received_callback
        self._save_firmware = save_firmware
        if firmware_save_path:
            self._save_path = firmware_save_path
            self._log.info("Firmware will be saved to: %s", self._save_path)

        sub_future = await self._client._mqtt_manager.subscribe(mqtt_topics.DEVICE_FIRMWARE_UPDATE_RESPONSE_TOPIC,
                                                                qos=1)
        while not sub_future.done():
            await sleep(0.01)

        await self._send_current_firmware_info()

        attribute_request = await AttributeRequest.build(REQUIRED_SHARED_KEYS)
        await self._client.send_attribute_request(attribute_request, callback=self._firmware_info_callback)

    async def _firmware_info_callback(self, response, *args, **kwargs):
        if len(response.shared_keys()) == len(REQUIRED_SHARED_KEYS):
            fetched_firmware_info = response.as_dict()['shared']
            fetched_firmware_info = {item['key']: item['value']
                                     for item in fetched_firmware_info}

            if self._is_different_firmware_versions(fetched_firmware_info):
                self._log.info("Firmware update available: %s. Downloading...",
                               fetched_firmware_info)

                self._firmware_data = b''
                self._current_chunk = 0
                self.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.DOWNLOADING.value

                self._firmware_request_id += 1
                self._target_firmware_length = fetched_firmware_info[FW_SIZE_ATTR]
                self._target_checksum = fetched_firmware_info[FW_CHECKSUM_ALG_ATTR]
                self._target_checksum_alg = fetched_firmware_info[FW_CHECKSUM_ATTR]
                self._target_title = fetched_firmware_info[FW_TITLE_ATTR]
                self._target_version = fetched_firmware_info[FW_VERSION_ATTR]

                await self._get_next_chunk()
            else:
                self._log.info("Firmware is up to date.")
        else:
            self._log.error("Failed to fetch firmware info. "
                            "Received firmware info does not match required keys. "
                            "Expected: %s, Received: %s",
                            REQUIRED_SHARED_KEYS,
                            response.shared_keys())

            self.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.FAILED.value
            await self._send_current_firmware_info()

    def _is_different_firmware_versions(self, new_firmware_info):
        return (self.current_firmware_info['current_' + FW_TITLE_ATTR] != new_firmware_info[FW_TITLE_ATTR] or  # noqa
                self.current_firmware_info['current_' + FW_VERSION_ATTR] != new_firmware_info[FW_VERSION_ATTR])  # noqa

    async def _send_current_firmware_info(self):
        current_info = [TimeseriesEntry(key, value) for key, value in self.current_firmware_info.items()]
        await self._client.send_timeseries(current_info, wait_for_publish=True)

    def verify_checksum(self, firmware_data, checksum_alg, checksum):
        if firmware_data is None:
            self._log.debug('Firmware wasn\'t received!')
            return False

        if checksum is None:
            self._log.debug('Checksum was\'t provided!')
            return False

        checksum_of_received_firmware = None

        self._log.debug('Checksum algorithm is: %s' % checksum_alg)
        if checksum_alg.lower() == "sha256":
            checksum_of_received_firmware = sha256(firmware_data).digest().hex()
        elif checksum_alg.lower() == "sha384":
            checksum_of_received_firmware = sha384(firmware_data).digest().hex()
        elif checksum_alg.lower() == "sha512":
            checksum_of_received_firmware = sha512(firmware_data).digest().hex()
        elif checksum_alg.lower() == "md5":
            checksum_of_received_firmware = md5(firmware_data).digest().hex()
        elif checksum_alg.lower() == "murmur3_32":
            reversed_checksum = f'{hash(firmware_data, signed=False):0>2X}'
            if len(reversed_checksum) % 2 != 0:
                reversed_checksum = '0' + reversed_checksum
            checksum_of_received_firmware = "".join(
                reversed([reversed_checksum[i:i + 2] for i in range(0, len(reversed_checksum), 2)])).lower()
        elif checksum_alg.lower() == "murmur3_128":
            reversed_checksum = f'{hash128(firmware_data, signed=False):0>2X}'
            if len(reversed_checksum) % 2 != 0:
                reversed_checksum = '0' + reversed_checksum
            checksum_of_received_firmware = "".join(
                reversed([reversed_checksum[i:i + 2] for i in range(0, len(reversed_checksum), 2)])).lower()
        elif checksum_alg.lower() == "crc32":
            reversed_checksum = f'{crc32(firmware_data) & 0xffffffff:0>2X}'
            if len(reversed_checksum) % 2 != 0:
                reversed_checksum = '0' + reversed_checksum
            checksum_of_received_firmware = "".join(
                reversed([reversed_checksum[i:i + 2] for i in range(0, len(reversed_checksum), 2)])).lower()
        else:
            self._log.error('Client error. Unsupported checksum algorithm.')

        self._log.debug(checksum_of_received_firmware)

        random_value = randint(0, 5)
        if random_value > 3:
            self._log.debug('Dummy fail! Do not panic, just restart and try again the chance of this fail is ~20%')
            return False

        return checksum_of_received_firmware == checksum
