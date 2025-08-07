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
from time import time
import requests


def get_device_attributes(device_id: str, base_url: str, headers: dict, scope: str) -> list:

    url = f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes?scope={scope}"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


def get_device_timeseries(device_id: str, base_url: str, headers: dict, keys:list =None) -> dict:
    current_ts = int(time() * 1000)

    if keys is None:
        keys = []

    url = (f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?startTs={current_ts - 60000}"
           f"&endTs={current_ts}"
           f"&useStrictDataTypes=true")

    if keys:
        url += f"&keys={','.join(keys)}"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def update_shared_attributes(device_id: str, base_url: str, headers: dict, attributes: dict) -> None:
    url = f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE"

    response = requests.post(url, json=attributes, headers=headers)

    if response.status_code != 200:
        response.raise_for_status()

def get_device_info_by_name(device_name: str, base_url: str, headers: dict) -> dict:
    url = f"{base_url}/api/tenant/devices?deviceName={device_name}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        response.raise_for_status()
    device = response.json()
    if not device:
        raise ValueError(f"Device with name '{device_name}' not found.")
    return device

def find_related_entity_id(device_id: str, base_url: str, headers: dict) -> str:
    url = f"{base_url}/api/relations?fromId={device_id}&fromType=DEVICE"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        relations = response.json()
        if relations:
            return relations[0]['to']['id']  # Return the first related entity
        else:
            raise ValueError(f"No related entity found for device ID '{device_id}'.")
    else:
        response.raise_for_status()

def get_device_info_by_id(device_id: str, base_url: str, headers: dict) -> dict:
    url = f"{base_url}/api/device/{device_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def get_default_device_profile(base_url: str, headers: dict) -> dict:
    url = f"{base_url}/api/deviceProfileInfo/default"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def create_device_profile_with_provisioning(device_profile_name: str,
                                            base_url: str,
                                            headers: dict,
                                            provisioning_device_key: str,
                                            provisioning_device_secret: str) -> dict:
    device_profile = get_default_device_profile(base_url, headers)
    url = f"{base_url}/api/deviceProfile"
    device_profile['id'] = None
    device_profile['name'] = device_profile_name
    device_profile['createdTime'] = None
    device_profile['provisionType'] = 'ALLOW_CREATE_NEW_DEVICES'
    if 'profileData' not in device_profile:
        device_profile['profileData'] = {
                                            "configuration": {
                                                "type": "DEFAULT"
                                            },
                                            "transportConfiguration": {
                                                "type": "DEFAULT"
                                            },
                                            "provisionConfiguration": {
                                                "type": "ALLOW_CREATE_NEW_DEVICES",
                                                "provisionDeviceSecret": provisioning_device_secret
                                            },
                                            "alarms": None
                                        }
    else:
        device_profile['profileData']['provisionConfiguration'] = {
            'type': 'ALLOW_CREATE_NEW_DEVICES',
            'provisionDeviceSecret': provisioning_device_secret
        }
    device_profile['provisionDeviceKey'] = provisioning_device_key

    response = requests.post(url, json=device_profile, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

async def send_rpc_request(device_id: str, base_url: str, headers: dict, request: dict):
    loop = asyncio.get_running_loop()

    def _send():
        url = f"{base_url}/api/rpc/twoway/{device_id}"
        r = requests.post(url, json=request, headers=headers, timeout=60)
        r.raise_for_status()
        return r.json()

    result = await loop.run_in_executor(None, _send)
    return result
