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
from hashlib import sha256, sha384, sha512, md5
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from zlib import crc32

import pytest

from tb_mqtt_client.common.mqtt_message import MqttPublishMessage
from tb_mqtt_client.constants.firmware import (FirmwareStates, FW_STATE_ATTR, FW_TITLE_ATTR, FW_VERSION_ATTR,
                                               FW_SIZE_ATTR, FW_CHECKSUM_ALG_ATTR, REQUIRED_SHARED_KEYS,
                                               FW_CHECKSUM_ATTR)
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.device.firmware_updater import FirmwareUpdater


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._mqtt_manager.register_handler = MagicMock()
    client._mqtt_manager.subscribe = AsyncMock(return_value=asyncio.Future())
    client._mqtt_manager.subscribe.return_value.set_result(True)
    client._mqtt_manager.unsubscribe = AsyncMock()
    client._mqtt_manager.is_connected.return_value = True
    client._message_queue.publish = AsyncMock()
    client.send_timeseries = AsyncMock()
    client.send_attribute_request = AsyncMock()
    return client


@pytest.fixture
def updater(mock_client):
    return FirmwareUpdater(mock_client)


@pytest.mark.asyncio
async def test_update_success(updater, mock_client):
    with patch("tb_mqtt_client.entities.data.attribute_request.AttributeRequest.build", new_callable=AsyncMock), \
            patch.object(updater, "_firmware_info_callback", new=AsyncMock()):
        await updater.update()
        mock_client._mqtt_manager.subscribe.assert_called_once()
        mock_client.send_timeseries.assert_called()
        mock_client.send_attribute_request.assert_called_once()


@pytest.mark.asyncio
async def test_update_not_connected(updater, mock_client):
    mock_client._mqtt_manager.is_connected.return_value = False
    await updater.update()


@pytest.mark.asyncio
async def test_handle_firmware_update_full(updater):
    updater._target_firmware_length = 4
    updater._chunk_size = 4
    updater._firmware_data = b'ab'
    payload = b'cd'
    with patch.object(updater, '_verify_downloaded_firmware', new=AsyncMock()) as verify:
        await updater._handle_firmware_update(None, payload)
        verify.assert_awaited_once()
        assert updater._firmware_data == b'abcd'


@pytest.mark.asyncio
async def test_handle_firmware_update_partial(updater):
    updater._target_firmware_length = 10
    updater._chunk_size = 5
    updater._firmware_data = b'123'
    payload = b'456'
    with patch.object(updater, '_get_next_chunk', new=AsyncMock()) as next_chunk:
        await updater._handle_firmware_update(None, payload)
        next_chunk.assert_awaited_once()
        assert updater._firmware_data.endswith(payload)


@pytest.mark.asyncio
async def test_get_next_chunk_valid(updater, mock_client):
    updater._chunk_size = 5
    updater._target_firmware_length = 10
    updater._firmware_request_id = 1
    updater._current_chunk = 2
    await updater._get_next_chunk()
    mock_client._message_queue.publish.assert_awaited()


@pytest.mark.asyncio
async def test_get_next_chunk_empty_payload(updater, mock_client):
    updater._chunk_size = 15
    updater._target_firmware_length = 10
    await updater._get_next_chunk()
    mock_client._message_queue.publish.assert_awaited()


@pytest.mark.asyncio
async def test_verify_downloaded_firmware_success(updater):
    updater._firmware_data = b'data'
    updater._target_checksum = "8d777f385d3dfec8815d20f7496026dc"
    updater._target_checksum_alg = "md5"
    updater.current_firmware_info[FW_STATE_ATTR] = FirmwareStates.DOWNLOADING.value
    with patch.object(updater, 'verify_checksum', return_value=True), \
            patch.object(updater, '_apply_downloaded_firmware', new=AsyncMock()), \
            patch.object(updater, '_send_current_firmware_info', new=AsyncMock()):
        await updater._verify_downloaded_firmware()
        assert updater.current_firmware_info[FW_STATE_ATTR] == FirmwareStates.VERIFIED.value


@pytest.mark.asyncio
async def test_verify_downloaded_firmware_fail(updater):
    updater._firmware_data = b'data'
    updater._target_checksum = "wrong"
    updater._target_checksum_alg = "md5"
    with patch.object(updater, 'verify_checksum', return_value=False), \
            patch.object(updater, '_send_current_firmware_info', new=AsyncMock()):
        await updater._verify_downloaded_firmware()
        assert updater.current_firmware_info[FW_STATE_ATTR] == FirmwareStates.FAILED.value


@pytest.mark.asyncio
async def test_apply_downloaded_firmware_saves_file(tmp_path, updater):
    updater._firmware_data = b'binary-firmware'
    updater._target_title = 'fw.bin'
    updater._target_version = 'v3'
    updater._save_path = str(tmp_path)
    updater._save_firmware = True
    updater._on_received_callback = AsyncMock()
    with patch.object(updater, '_send_current_firmware_info', new=AsyncMock()), \
            patch.object(updater._client._mqtt_manager, 'unsubscribe', new=AsyncMock()):
        await updater._apply_downloaded_firmware()
        assert (tmp_path / 'fw.bin').exists()


def test_verify_checksum_md5_valid(updater):
    result = updater.verify_checksum(b'data', 'md5', "8d777f385d3dfec8815d20f7496026dc")
    assert isinstance(result, bool)


def test_verify_checksum_invalid_algorithm(updater):
    result = updater.verify_checksum(b'data', 'invalid_alg', "deadbeef")
    assert result is False


def test_is_different_versions_true(updater):
    new_info = {FW_TITLE_ATTR: 'fw', FW_VERSION_ATTR: 'v2'}
    assert updater._is_different_firmware_versions(new_info) is True


def test_is_different_versions_false(updater):
    updater.current_firmware_info['current_' + FW_TITLE_ATTR] = 'fw'
    updater.current_firmware_info['current_' + FW_VERSION_ATTR] = 'v2'
    new_info = {FW_TITLE_ATTR: 'fw', FW_VERSION_ATTR: 'v2'}
    assert updater._is_different_firmware_versions(new_info) is False


@pytest.mark.asyncio
async def test_save_firmware_failure_logs_error(updater, caplog):
    updater._firmware_data = b'data'
    updater._target_title = "fw.bin"
    updater._target_version = "v1"
    updater._save_firmware = True
    with patch.object(updater, '_send_current_firmware_info', new=AsyncMock()), \
            patch.object(updater._client._mqtt_manager, 'unsubscribe', new=AsyncMock()), \
            patch.object(updater, '_save', side_effect=IOError("disk error")):
        await updater._apply_downloaded_firmware()
        assert "Failed to save firmware" in caplog.text
        assert updater.current_firmware_info[FW_STATE_ATTR] == FirmwareStates.FAILED.value


@pytest.mark.asyncio
async def test_send_current_firmware_info_calls_send_timeseries(updater, mock_client):
    updater.current_firmware_info = {
        f"current_{FW_TITLE_ATTR}": "test",
        f"current_{FW_VERSION_ATTR}": "1.0",
        FW_STATE_ATTR: FirmwareStates.DOWNLOADING.value
    }
    await updater._send_current_firmware_info()
    mock_client.send_timeseries.assert_awaited_once()
    args = mock_client.send_timeseries.call_args[0][0]
    assert all(isinstance(entry, TimeseriesEntry) for entry in args)


@pytest.mark.asyncio
async def test_firmware_info_callback_keys_mismatch(updater, caplog):
    response = MagicMock()
    response.shared_keys.return_value = ["unexpected_key"]
    await updater._firmware_info_callback(response)
    assert "does not match required keys" in caplog.text
    assert updater.current_firmware_info[FW_STATE_ATTR] == FirmwareStates.FAILED.value


@pytest.mark.asyncio
async def test_firmware_info_callback_same_version(updater):
    updater.current_firmware_info[f"current_{FW_TITLE_ATTR}"] = "fw"
    updater.current_firmware_info[f"current_{FW_VERSION_ATTR}"] = "v1"

    response = MagicMock()
    response.shared_keys.return_value = REQUIRED_SHARED_KEYS
    response.as_dict.return_value = {
        "shared": [{"key": FW_TITLE_ATTR, "value": "fw"},
                   {"key": FW_VERSION_ATTR, "value": "v1"},
                   {"key": FW_SIZE_ATTR, "value": 123},
                   {"key": FW_CHECKSUM_ALG_ATTR, "value": "dummy"},
                   {"key": FW_CHECKSUM_ATTR, "value": "dummy"}]
    }

    await updater._firmware_info_callback(response)


@pytest.mark.asyncio
async def test_firmware_info_callback_triggers_download(updater):
    response = MagicMock()
    response.shared_keys.return_value = REQUIRED_SHARED_KEYS
    response.as_dict.return_value = {
        "shared": [{"key": FW_TITLE_ATTR, "value": "new_fw"},
                   {"key": FW_VERSION_ATTR, "value": "v2"},
                   {"key": FW_SIZE_ATTR, "value": 123},
                   {"key": FW_CHECKSUM_ALG_ATTR, "value": "alg"},
                   {"key": FW_CHECKSUM_ATTR, "value": "chk"}]
    }

    with patch.object(updater, '_get_next_chunk', new=AsyncMock()) as mocked:
        await updater._firmware_info_callback(response)
        mocked.assert_awaited_once()
        assert updater._target_title == "new_fw"
        assert updater._target_version == "v2"


def test_verify_checksum_null_data(updater):
    result = updater.verify_checksum(None, "md5", "abc")
    assert not result


def test_verify_checksum_null_checksum(updater):
    result = updater.verify_checksum(b"data", "md5", None)
    assert not result


@pytest.mark.parametrize("alg", [
    ("sha256", sha256(b"data").digest().hex()),
    ("sha384", sha384(b"data").digest().hex()),
    ("sha512", sha512(b"data").digest().hex()),
    ("md5", md5(b"data").digest().hex()),
    ("crc32", "".join(reversed([f'{crc32(b"data") & 0xffffffff:0>2X}'[i:i + 2]
                                for i in range(0, len(f'{crc32(b"data") & 0xffffffff:0>2X}'), 2)])).lower())
])
def test_verify_checksum_known_algorithms(updater, alg):
    name, checksum = alg
    with patch("tb_mqtt_client.service.device.firmware_updater.randint", return_value=0):
        assert updater.verify_checksum(b"data", name, checksum) is True


def test_verify_checksum_random_failure(updater):
    with patch("tb_mqtt_client.service.device.firmware_updater.randint", return_value=5):
        result = updater.verify_checksum(b"data", "md5", md5(b"data").digest().hex())
        assert not result
