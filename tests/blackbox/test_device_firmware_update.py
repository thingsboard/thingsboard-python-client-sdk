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
from copy import deepcopy

import pytest

from examples.device import firmware_update
from tests.blackbox.rest_helpers import save_device


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_device_firmware_update(firmware_profile_and_package, device_info, device_config, tb_admin_headers, test_config):

    handler_future = asyncio.Future()

    async def firmware_update_callback(firmware_data, current_firmware_info):
        handler_future.set_result((firmware_data, current_firmware_info))

    firmware_update.config = device_config
    firmware_update.firmware_update_callback = firmware_update_callback

    device_profile, firmware_package, firmware_info = firmware_profile_and_package

    device = deepcopy(device_info)

    device['deviceProfileId'] = device_profile['id']
    device['firmwareId'] = firmware_package['id']

    save_device(device, test_config['tb_url'], tb_admin_headers)

    await asyncio.sleep(1)  # Ensure the device is saved before starting the firmware update

    task = asyncio.create_task(firmware_update.main())
    result = None
    try:
        result = await asyncio.wait_for(handler_future, timeout=30)
    except Exception as e:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert result is not None, "Firmware update callback should be called"
    assert isinstance(result, tuple), "Result should be a tuple containing firmware data and info"
    received_firmware_data, received_firmware_info = result
    assert received_firmware_info['current_fw_title'] == firmware_info['name']
    assert received_firmware_info['current_fw_version'] == firmware_info['version'], \
        "Received firmware version should match the package version"
    assert received_firmware_info['fw_state'] == 'UPDATED'
