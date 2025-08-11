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

from __future__ import annotations

import asyncio
from time import time as now
from typing import Dict, List, Optional, Tuple

import requests


def _check_ok(resp: requests.Response) -> None:
    if not resp.ok:
        resp.raise_for_status()


def get_device_attributes(
        device_id: str, base_url: str, headers: Dict[str, str], scope: str, *, http: Optional[requests.Session] = None,
        timeout: float = 30.0
) -> List[dict]:
    """Fetch device attributes for a scope."""
    sess = http or requests
    url = f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes"
    resp = sess.get(url, headers=headers, params={"scope": scope}, timeout=timeout)
    _check_ok(resp)
    return resp.json()


def get_device_timeseries(
        device_id: str,
        base_url: str,
        headers: Dict[str, str],
        keys: Optional[List[str]] = None,
        *,
        http: Optional[requests.Session] = None,
        timeout: float = 30.0,
) -> Dict[str, list]:
    """Fetch device timeseries for the last 60 seconds, optionally filtered by keys."""
    sess = http or requests
    current_ts = int(now() * 1000)
    params = {
        "startTs": current_ts - 60_000,
        "endTs": current_ts,
        "useStrictDataTypes": "true",
    }
    if keys:
        params["keys"] = ",".join(keys)
    url = f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
    resp = sess.get(url, headers=headers, params=params, timeout=timeout)
    _check_ok(resp)
    return resp.json()


def update_shared_attributes(
        device_id: str, base_url: str, headers: Dict[str, str], attributes: Dict[str, object], *,
        http: Optional[requests.Session] = None, timeout: float = 30.0
) -> None:
    """Update shared scope attributes for a device."""
    sess = http or requests
    url = f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE"
    resp = sess.post(url, json=attributes, headers=headers, timeout=timeout)
    _check_ok(resp)


def get_device_info_by_name(
        device_name: str, base_url: str, headers: Dict[str, str], *, http: Optional[requests.Session] = None,
        timeout: float = 30.0
) -> dict:
    """Look up a device by its exact name."""
    sess = http or requests
    url = f"{base_url}/api/tenant/devices"
    resp = sess.get(url, headers=headers, params={"deviceName": device_name}, timeout=timeout)
    _check_ok(resp)
    device = resp.json()
    if not device:
        raise ValueError(f"Device with name '{device_name}' not found.")
    return device


def find_related_entity_id(
        device_id: str, base_url: str, headers: Dict[str, str], *, http: Optional[requests.Session] = None,
        timeout: float = 30.0
) -> str:
    """Return the first related entity ID for a device (if any)."""
    sess = http or requests
    url = f"{base_url}/api/relations"
    resp = sess.get(url, headers=headers, params={"fromId": device_id, "fromType": "DEVICE"}, timeout=timeout)
    _check_ok(resp)
    relations = resp.json()
    if relations:
        return relations[0]["to"]["id"]
    raise ValueError(f"No related entity found for device ID '{device_id}'.")


def get_device_info_by_id(
        device_id: str, base_url: str, headers: Dict[str, str], *, http: Optional[requests.Session] = None,
        timeout: float = 30.0
) -> dict:
    """Fetch device info by ThingsBoard device UUID."""
    sess = http or requests
    url = f"{base_url}/api/device/{device_id}"
    resp = sess.get(url, headers=headers, timeout=timeout)
    _check_ok(resp)
    return resp.json()


def get_default_device_profile(
        base_url: str, headers: Dict[str, str], *, http: Optional[requests.Session] = None, timeout: float = 30.0
) -> dict:
    """Get the tenant default device profile (info)."""
    sess = http or requests
    url = f"{base_url}/api/deviceProfileInfo/default"
    resp = sess.get(url, headers=headers, timeout=timeout)
    _check_ok(resp)
    return resp.json()


def create_device_profile_with_provisioning(
        device_profile_name: str,
        base_url: str,
        headers: Dict[str, str],
        provisioning_device_key: str,
        provisioning_device_secret: str,
        *,
        http: Optional[requests.Session] = None,
        timeout: float = 30.0,
) -> dict:
    """Create a device profile with ALLOW_CREATE_NEW_DEVICES provisioning."""
    sess = http or requests
    device_profile = get_default_device_profile(base_url, headers, http=sess, timeout=timeout)
    device_profile.update(
        {
            "id": None,
            "name": device_profile_name,
            "createdTime": None,
            "provisionType": "ALLOW_CREATE_NEW_DEVICES",
        }
    )
    profile_data = device_profile.setdefault(
        "profileData",
        {
            "configuration": {"type": "DEFAULT"},
            "transportConfiguration": {"type": "DEFAULT"},
            "alarms": None,
        },
    )
    profile_data["provisionConfiguration"] = {
        "type": "ALLOW_CREATE_NEW_DEVICES",
        "provisionDeviceSecret": provisioning_device_secret,
    }
    device_profile["provisionDeviceKey"] = provisioning_device_key

    url = f"{base_url}/api/deviceProfile"
    resp = sess.post(url, json=device_profile, headers=headers, timeout=timeout)
    _check_ok(resp)
    return resp.json()


def create_device_profile_and_firmware(
        firmware_name: str,
        firmware_version: str,
        firmware_bytes: bytes,
        base_url: str,
        headers: Dict[str, str],
        *,
        http: Optional[requests.Session] = None,
        timeout: float = 30.0,
) -> Tuple[dict, dict]:
    """
    Create a device profile and upload a firmware OTA package.

    Returns:
        (device_profile, firmware_ota_package)
    """
    sess = http or requests

    # Create device profile
    device_profile = get_default_device_profile(base_url, headers, http=sess, timeout=timeout)
    device_profile.update({"id": None, "createdTime": None, "name": "pytest-firmware-profile"})
    device_profile.setdefault("profileData", {
        "configuration": {"type": "DEFAULT"},
        "transportConfiguration": {"type": "DEFAULT"},
    })

    resp = sess.post(f"{base_url}/api/deviceProfile", json=device_profile, headers=headers, timeout=timeout)
    _check_ok(resp)
    device_profile = resp.json()

    # Create OTA package (metadata)
    init_ota_package = {
        "id": None,
        "createdTime": None,
        "deviceProfileId": {"entityType": "DEVICE_PROFILE", "id": device_profile["id"]["id"]},
        "type": "FIRMWARE",
        "title": firmware_name,
        "version": firmware_version,
        "tag": f"{firmware_name} {firmware_version}",
        "url": None,
        "hasData": False,
        "fileName": None,
        "contentType": None,
        "checksumAlgorithm": None,
        "checksum": None,
        "dataSize": None,
        "externalId": None,
        "name": firmware_name,
        "additionalInfo": {"description": ""},
    }
    created_ota_package = sess.post(f"{base_url}/api/otaPackage", json=init_ota_package, headers=headers,
                                    timeout=timeout)
    _check_ok(created_ota_package)
    initial = created_ota_package.json()

    # Upload firmware data using proper multipart form-data
    upload_url = f"{base_url}/api/otaPackage/{initial['id']['id']}?checksumAlgorithm=SHA256"
    files = {
        "file": ("firmware.bin", firmware_bytes, "application/octet-stream"),
    }
    # Only auth header required; requests builds Content-Type with boundary.
    upload_headers = {k: v for k, v in headers.items() if k.lower() == "x-authorization"}
    upload_resp = sess.post(upload_url, headers=upload_headers, files=files, timeout=timeout)
    _check_ok(upload_resp)

    return device_profile, upload_resp.json()


def save_device(
        device: dict, base_url: str, headers: Dict[str, str], *, http: Optional[requests.Session] = None,
        timeout: float = 30.0
) -> dict:
    """Create or update a device via REST."""
    sess = http or requests
    url = f"{base_url}/api/device"
    resp = sess.post(url, json=device, headers=headers, timeout=timeout)
    _check_ok(resp)
    return resp.json()


async def send_rpc_request(
        device_id: str,
        base_url: str,
        headers: Dict[str, str],
        request: dict,
        *,
        http: Optional[requests.Session] = None,
        timeout: float = 60.0,
) -> dict:
    """
    Send a two-way RPC request to a device. Performs the blocking HTTP call in a thread pool.
    """
    sess = http or requests

    def _send() -> dict:
        url = f"{base_url}/api/rpc/twoway/{device_id}"
        r = sess.post(url, json=request, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _send)
