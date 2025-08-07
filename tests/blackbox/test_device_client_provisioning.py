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

import pytest
import requests

from examples.device import client_provisioning
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, AccessTokenProvisioningCredentials
from tests.blackbox.rest_helpers import create_device_profile_with_provisioning, \
    get_device_timeseries, get_device_info_by_name


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_client_provisioning(tb_admin_headers, test_config):
    device_name = 'pytest-provisioned-device'

    provisioning_device_key = 'pytest-provisioning-device-key'
    provisioning_device_secret = 'pytest-provisioning-device-secret'

    provisioning_credentials = AccessTokenProvisioningCredentials(
        provision_device_key=provisioning_device_key,
        provision_device_secret=provisioning_device_secret,
    )
    provisioning_request = ProvisioningRequest(test_config['tb_host'],
                                               credentials=provisioning_credentials,
                                               device_name=device_name)

    device_profile = create_device_profile_with_provisioning('pytest-provisioning-profile',
                                                             test_config['tb_url'],
                                                             tb_admin_headers,
                                                             provisioning_device_key,
                                                             provisioning_device_secret)
    provisioned_device_info = None
    try:
        client_provisioning.provisioning_request = provisioning_request

        try:
            await client_provisioning.main()
        except Exception as e:
            pytest.fail(f"Provisioning failed with exception: {e}")

        provisioned_device_info = get_device_info_by_name(device_name,
                                                          test_config['tb_url'],
                                                          tb_admin_headers)

        assert provisioned_device_info is not None, "Provisioned device info should not be None"
        assert provisioned_device_info['name'] == device_name, "Provisioned device name should match"
        assert provisioned_device_info['deviceProfileId']['id'] == device_profile['id']['id'], \
            "Provisioned device profile ID should match"

        timeseries = get_device_timeseries(provisioned_device_info['id']['id'], test_config['tb_url'], tb_admin_headers,
                                           ['batteryLevel'])
        assert len(timeseries) == 1, "There should be one timeseries entry"
        assert 'batteryLevel' in timeseries, "Timeseries key should be 'batteryLevel'"
        assert 'value' in timeseries['batteryLevel'][0], "Timeseries entry should have a value"
    finally:
        # Cleanup: delete the provisioned device and profile
        if provisioned_device_info and 'id' in provisioned_device_info:
            device_id = provisioned_device_info['id']['id']
            delete_url = f"{test_config['tb_url']}/api/device/{device_id}"
            requests.delete(delete_url, headers=tb_admin_headers)

        if 'id' in device_profile:
            profile_id = device_profile['id']['id']
            delete_profile_url = f"{test_config['tb_url']}/api/deviceProfile/{profile_id}"
            requests.delete(delete_profile_url, headers=tb_admin_headers)
