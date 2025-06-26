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

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from tb_mqtt_client.service.device.firmware_updater import FirmwareUpdater
from tb_mqtt_client.constants.firmware import FW_TITLE_ATTR, FW_VERSION_ATTR, FW_STATE_ATTR, FirmwareStates


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._mqtt_manager.register_handler = MagicMock()
    client._mqtt_manager.subscribe = AsyncMock(return_value=asyncio.Future())
    client._mqtt_manager.subscribe.return_value.set_result(True)
    client._mqtt_manager.unsubscribe = AsyncMock()
    client._mqtt_manager.is_connected.return_value = True
    client._message_queue.publish = AsyncMock()
    client.send_telemetry = AsyncMock()
    client.send_attribute_request = AsyncMock()
    return client

@pytest.fixture
def updater(mock_client):
    return FirmwareUpdater(mock_client)

@pytest.mark.asyncio
async def test_update_success(updater, mock_client):
    with patch("tb_mqtt_client.entities.data.attribute_request.AttributeRequest.build", new_callable=AsyncMock) as mock_build, \
         patch.object(updater, "_firmware_info_callback", new=AsyncMock()):
        await updater.update()
        mock_client._mqtt_manager.subscribe.assert_called_once()
        mock_client.send_telemetry.assert_called()
        mock_client.send_attribute_request.assert_called_once()

@pytest.mark.asyncio
async def test_update_not_connected(updater, mock_client, caplog):
    mock_client._mqtt_manager.is_connected.return_value = False
    await updater.update()
    assert "Client is not connected" in caplog.text

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
    mock_client._message_queue.publish.assert_awaited_with(
        topic=ANY,
        payload=b'',
        datapoints_count=0,
        qos=1
    )

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

def test_verify_checksum_invalid_algorithm(updater, caplog):
    result = updater.verify_checksum(b'data', 'invalid_alg', "deadbeef")
    assert result is False
    assert 'Unsupported checksum algorithm' in caplog.text

def test_is_different_versions_true(updater):
    new_info = {FW_TITLE_ATTR: 'fw', FW_VERSION_ATTR: 'v2'}
    assert updater._is_different_firmware_versions(new_info) is True

def test_is_different_versions_false(updater):
    updater.current_firmware_info['current_' + FW_TITLE_ATTR] = 'fw'
    updater.current_firmware_info['current_' + FW_VERSION_ATTR] = 'v2'
    new_info = {FW_TITLE_ATTR: 'fw', FW_VERSION_ATTR: 'v2'}
    assert updater._is_different_firmware_versions(new_info) is False


if __name__ == '__main__':
    pytest.main([__file__])
