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

import os
import subprocess
import time
import pytest
import requests
import logging

from requests import HTTPError

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.common.config_loader import DeviceConfig
from tests.blackbox.rest_helpers import find_related_entity_id, get_device_info_by_id

TB_HOST = os.getenv("SDK_BLACKBOX_TB_HOST", "localhost")
TB_HTTP_PORT = int(os.getenv("SDK_BLACKBOX_TB_PORT", 8080))
TB_MQTT_PORT = int(os.getenv("SDK_BLACKBOX_TB_MQTT_PORT", 1883))
TENANT_USER = os.getenv("SDK_BLACKBOX_TENANT_USER", "tenant@thingsboard.org")
TENANT_PASS = os.getenv("SDK_BLACKBOX_TENANT_PASS", "tenant")
TB_CONTAINER_NAME = os.getenv("SDK_BLACKBOX_TB_CONTAINER", "tb-ce-sdk-tests")
RUN_WITH_LOCAL_TB = os.getenv("SDK_RUN_BLACKBOX_TESTS_LOCAL", "false").lower() == "true"

TB_HTTP_PROTOCOL = os.getenv("SDK_BLACKBOX_TB_HTTP_PROTOCOL", "http")
TB_URL = os.getenv("SDK_BLACKBOX_TB_URL", f"{TB_HTTP_PROTOCOL}://{TB_HOST}:{TB_HTTP_PORT}")

logger = logging.getLogger("blackbox")
logger.setLevel(logging.INFO)


def pytest_collection_modifyitems(config, items):
    """Skip blackbox tests unless explicitly enabled."""
    if os.getenv("SDK_RUN_BLACKBOX_TESTS", "false").lower() != "true":
        skip_marker = pytest.mark.skip(reason="Blackbox tests disabled. Set SDK_RUN_BLACKBOX_TESTS=true to run.")
        for item in items:
            if "blackbox" in str(item.fspath):
                item.add_marker(skip_marker)


@pytest.fixture(scope="session", autouse=True)
def start_thingsboard():
    """Start ThingsBoard CE in Docker if blackbox tests are enabled."""
    if os.getenv("SDK_RUN_BLACKBOX_TESTS", "false").lower() != "true":
        return

    if not RUN_WITH_LOCAL_TB:
        try:
            status = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", TB_CONTAINER_NAME],
                stderr=subprocess.DEVNULL
            ).decode().strip()
            if status == "true":
                logger.info(f"Using existing ThingsBoard CE container: {TB_CONTAINER_NAME}")
                wait_for_tb_ready()
                return
        except subprocess.CalledProcessError:
            pass

        logger.info("Pulling latest ThingsBoard CE image...")
        subprocess.run(["docker", "pull", "thingsboard/tb-postgres"], check=True)

        logger.info("Starting ThingsBoard CE container...")
        subprocess.run([
            "docker", "run", "-d", "--rm",
            "--name", TB_CONTAINER_NAME,
            "-p", "8080:8080",
            "-p", f"{TB_MQTT_PORT}:1883",
            "thingsboard/tb-postgres"
        ], check=True)

    wait_for_tb_ready()


def wait_for_tb_ready():
    logger.info("Waiting for ThingsBoard CE to become ready...")
    for _ in range(60):
        try:
            r = requests.post(f"{TB_URL}/api/auth/login", json={
                "username": TENANT_USER,
                "password": TENANT_PASS
            })
            if r.ok:
                logger.info("ThingsBoard CE is ready.")
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    pytest.fail("ThingsBoard CE did not become ready in time.")


@pytest.fixture(scope="session")
def test_config():
    """Return configuration for blackbox tests."""
    return {
        "tb_host": TB_HOST,
        "tb_http_port": TB_HTTP_PORT,
        "tb_mqtt_port": TB_MQTT_PORT,
        "tenant_user": TENANT_USER,
        "tenant_pass": TENANT_PASS,
        "tb_url": TB_URL
    }


@pytest.fixture(scope="session")
def tb_admin_token(start_thingsboard):
    """Login as tenant admin and return JWT."""
    r = requests.post(f"{TB_URL}/api/auth/login", json={
        "username": TENANT_USER,
        "password": TENANT_PASS
    })
    r.raise_for_status()
    return r.json()["token"]


@pytest.fixture(scope="session")
def tb_admin_headers(tb_admin_token):
    """Return headers for ThingsBoard REST API requests."""
    return {
        "X-Authorization": f"Bearer {tb_admin_token}",
        "Content-Type": "application/json"
    }


@pytest.fixture()
def device_info(tb_admin_headers):
    try:
        r = requests.post(f"{TB_URL}/api/device", headers=tb_admin_headers, json={
            "name": "pytest-device",
            "type": "default"
        })
        r.raise_for_status()
        device = r.json()
    except HTTPError as e:
        logger.error(f"Failed to create device: {e}")
        r = requests.get(f"{TB_URL}/api/tenant/devices?deviceName=pytest-device", headers=tb_admin_headers)
        r.raise_for_status()
        device = r.json()
        if not device:
            pytest.fail("Failed to create or find test device.")

    yield device


@pytest.fixture()
def device_config(device_info, tb_admin_headers):

    device_id = device_info["id"]["id"]

    # Get credentials
    r = requests.get(f"{TB_URL}/api/device/{device_id}/credentials", headers=tb_admin_headers)
    r.raise_for_status()
    token = r.json()["credentialsId"]

    device_config = DeviceConfig()
    device_config.host = TB_HOST
    device_config.port = TB_MQTT_PORT
    device_config.access_token = token
    yield device_config

    # Cleanup
    requests.delete(f"{TB_URL}/api/device/{device_id}", headers=tb_admin_headers)


@pytest.fixture()
def gateway_info(tb_admin_headers):
    try:
        r = requests.post(f"{TB_URL}/api/device", headers=tb_admin_headers, json={
            "name": "pytest-gw-device",
            "type": "default",
            "additionalInfo": {
                "gateway": True
            }
        })
        r.raise_for_status()
        device = r.json()
    except HTTPError as e:
        logger.error(f"Failed to create device: {e}")
        r = requests.get(f"{TB_URL}/api/tenant/devices?deviceName=pytest-gw-device", headers=tb_admin_headers)
        r.raise_for_status()
        device = r.json()
        if not device:
            pytest.fail("Failed to create or find test device.")

    yield device

@pytest.fixture
def gateway_config(gateway_info, tb_admin_headers):

    device_id = gateway_info["id"]["id"]

    # Get credentials
    r = requests.get(f"{TB_URL}/api/device/{device_id}/credentials", headers=tb_admin_headers)
    r.raise_for_status()
    token = r.json()["credentialsId"]

    gateway_config = GatewayConfig()
    gateway_config.host = TB_HOST
    gateway_config.port = TB_MQTT_PORT
    gateway_config.access_token = token
    yield gateway_config

    subdevice_id = find_related_entity_id(device_id, TB_URL, tb_admin_headers)

    # Cleanup
    requests.delete(f"{TB_URL}/api/device/{device_id}", headers=tb_admin_headers)

    sub_device_info = get_device_info_by_id(
        subdevice_id,
        TB_URL,
        tb_admin_headers
    )

    requests.delete(f"{TB_URL}/api/device/{subdevice_id}", headers=tb_admin_headers)

    requests.delete(f"{TB_URL}/api/deviceProfile/{sub_device_info['deviceProfileId']['id']}",
                    headers=tb_admin_headers)
