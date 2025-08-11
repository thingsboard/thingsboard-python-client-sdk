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

import logging
import os
import subprocess
import time
import uuid
from copy import deepcopy
from typing import Dict, Generator, Optional

import pytest
import requests
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from tb_mqtt_client.common.config_loader import DeviceConfig, GatewayConfig
from tests.blackbox.constants import RPC_RESPONSE_RULE_CHAIN
from tests.blackbox.rest_helpers import (
    find_related_entity_id,
    get_device_info_by_id,
    create_device_profile_and_firmware,
    get_default_device_profile,
)

# ----------------------------
# Environment and configuration
# ----------------------------

ENV = os.environ.get
TB_HOST: str = ENV("SDK_BLACKBOX_TB_HOST", "localhost")
TB_HTTP_PORT: int = int(ENV("SDK_BLACKBOX_TB_PORT", "8080"))
TB_MQTT_PORT: int = int(ENV("SDK_BLACKBOX_TB_MQTT_PORT", "1883"))
TENANT_USER: str = ENV("SDK_BLACKBOX_TENANT_USER", "tenant@thingsboard.org")
TENANT_PASS: str = ENV("SDK_BLACKBOX_TENANT_PASS", "tenant")
TB_CONTAINER_NAME: str = ENV("SDK_BLACKBOX_TB_CONTAINER", "tb-ce-sdk-tests")
RUN_WITH_LOCAL_TB: bool = ENV("SDK_RUN_BLACKBOX_TESTS_LOCAL", "false").lower() == "true"
RUN_BLACKBOX: bool = ENV("SDK_RUN_BLACKBOX_TESTS", "false").lower() == "true"

TB_HTTP_PROTOCOL: str = ENV("SDK_BLACKBOX_TB_HTTP_PROTOCOL", "http")
TB_URL: str = ENV("SDK_BLACKBOX_TB_URL", f"{TB_HTTP_PROTOCOL}://{TB_HOST}:{TB_HTTP_PORT}")

REQUEST_TIMEOUT: float = float(ENV("SDK_BLACKBOX_HTTP_TIMEOUT", "30"))
TB_START_TIMEOUT: int = int(ENV("SDK_BLACKBOX_TB_START_TIMEOUT", "180"))

logger = logging.getLogger("blackbox")
logger.setLevel(logging.INFO)


def _build_retrying_session() -> requests.Session:
    """A single session with retry/backoff for better stability in tests."""
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,  # exponential: 0.5, 1, 2, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST", "PUT", "DELETE", "PATCH"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@pytest.fixture(scope="session")
def http() -> Generator[requests.Session, None, None]:
    """Shared HTTP session with retries and connection pooling."""
    session = _build_retrying_session()
    try:
        yield session
    finally:
        session.close()


def pytest_collection_modifyitems(config, items):
    """Skip blackbox tests unless explicitly enabled."""
    if not RUN_BLACKBOX:
        skip_marker = pytest.mark.skip(
            reason=(
                "Blackbox tests disabled. Set SDK_RUN_BLACKBOX_TESTS=true to run. "
                "Optionally set SDK_RUN_BLACKBOX_TESTS_LOCAL=true to run against a local ThingsBoard instance."
            )
        )
        for item in items:
            if "blackbox" in str(item.fspath):
                item.add_marker(skip_marker)


# ----------------------------
# ThingsBoard lifecycle helpers
# ----------------------------

def _docker_is_running(container_name: str) -> bool:
    try:
        status = (
            subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
        return status == "true"
    except subprocess.CalledProcessError:
        return False


def _wait_for_tb_ready(http: requests.Session) -> None:
    """Wait for TB to accept logins; uses exponential backoff."""
    logger.info("Waiting for ThingsBoard CE to become ready...")
    start = time.time()
    delay = 1.0
    # Try login to ensure full readiness (DB + REST).
    while time.time() - start < TB_START_TIMEOUT:
        try:
            r = http.post(
                f"{TB_URL}/api/auth/login",
                json={"username": TENANT_USER, "password": TENANT_PASS},
                timeout=REQUEST_TIMEOUT,
            )
            if r.ok:
                logger.info("ThingsBoard CE is ready.")
                return
        except requests.RequestException:
            pass
        time.sleep(delay)
        delay = min(delay * 1.5, 5.0)
    pytest.fail("ThingsBoard CE did not become ready in time.")


@pytest.fixture(scope="session", autouse=True)
def start_thingsboard(http: requests.Session) -> Generator[None, None, None]:
    """
    Start ThingsBoard CE in Docker if blackbox tests are enabled and not running locally.
    Ensures instance is ready. If we start the container, we will stop it at session end.
    """
    if not RUN_BLACKBOX:
        _wait_for_tb_ready(http)
        yield
        return

    started_by_us = False

    if not RUN_WITH_LOCAL_TB:
        if _docker_is_running(TB_CONTAINER_NAME):
            logger.info("Using existing ThingsBoard CE container: %s", TB_CONTAINER_NAME)
        else:
            logger.info("Pulling ThingsBoard CE image (thingsboard/tb-postgres)...")
            subprocess.run(["docker", "pull", "thingsboard/tb-postgres"], check=True)

            logger.info("Starting ThingsBoard CE container...")
            subprocess.run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--rm",
                    "--name",
                    TB_CONTAINER_NAME,
                    "-p",
                    f"{TB_HTTP_PORT}:8080",
                    "-p",
                    f"{TB_MQTT_PORT}:1883",
                    "thingsboard/tb-postgres",
                ],
                check=True,
            )
            started_by_us = True

    _wait_for_tb_ready(http)

    try:
        yield
    finally:
        if started_by_us:
            logger.info("Stopping ThingsBoard CE container: %s", TB_CONTAINER_NAME)
            subprocess.run(["docker", "stop", TB_CONTAINER_NAME], check=False)


# ----------------------------
# Auth and headers
# ----------------------------

@pytest.fixture(scope="session")
def test_config() -> Dict[str, object]:
    """Return configuration for blackbox tests."""
    return {
        "tb_host": TB_HOST,
        "tb_http_port": TB_HTTP_PORT,
        "tb_mqtt_port": TB_MQTT_PORT,
        "tenant_user": TENANT_USER,
        "tenant_pass": TENANT_PASS,
        "tb_url": TB_URL,
        "timeout": REQUEST_TIMEOUT,
    }


@pytest.fixture(scope="session")
def tb_admin_token(start_thingsboard, http: requests.Session) -> str:
    """Login as tenant admin and return JWT."""
    r = http.post(
        f"{TB_URL}/api/auth/login",
        json={"username": TENANT_USER, "password": TENANT_PASS},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    # Some TB builds return {"token": "..."}; others include "refreshToken". We only need the token.
    return data["token"]


@pytest.fixture(scope="session")
def tb_admin_headers(tb_admin_token: str) -> Dict[str, str]:
    """Headers for ThingsBoard REST API requests."""
    return {
        "X-Authorization": f"Bearer {tb_admin_token}",
        "Content-Type": "application/json",
    }


# ----------------------------
# Device fixtures
# ----------------------------

def _create_device(http: requests.Session, headers: Dict[str, str], name: str, dev_type: str = "default") -> dict:
    r = http.post(
        f"{TB_URL}/api/device",
        headers=headers,
        json={"name": name, "type": dev_type},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def _get_device_credentials_token(http: requests.Session, headers: Dict[str, str], device_id: str) -> str:
    r = http.get(f"{TB_URL}/api/device/{device_id}/credentials", headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()["credentialsId"]


@pytest.fixture()
def device_info(tb_admin_headers: Dict[str, str], http: requests.Session) -> Generator[dict, None, None]:
    """Create a unique device for a test; always cleaned up."""
    name = f"pytest-device-{uuid.uuid4().hex[:8]}"
    try:
        device = _create_device(http, tb_admin_headers, name)
    except HTTPError as e:
        logger.error("Failed to create device %s: %s", name, e)
        pytest.fail("Failed to create test device.")

    try:
        yield device
    finally:
        device_id = device["id"]["id"]
        http.delete(f"{TB_URL}/api/device/{device_id}", headers=tb_admin_headers, timeout=REQUEST_TIMEOUT)


@pytest.fixture()
def device_config(
        device_info: dict, tb_admin_headers: Dict[str, str], http: requests.Session
) -> Generator[DeviceConfig, None, None]:
    """Return a ready-to-use DeviceConfig; cleans up the device after use via device_info fixture."""
    device_id = device_info["id"]["id"]
    token = _get_device_credentials_token(http, tb_admin_headers, device_id)

    cfg = DeviceConfig()
    cfg.host = TB_HOST
    cfg.port = TB_MQTT_PORT
    cfg.access_token = token
    yield cfg


# ----------------------------
# Gateway fixtures
# ----------------------------

def _create_gateway_device(http: requests.Session, headers: Dict[str, str], name: str) -> dict:
    r = http.post(
        f"{TB_URL}/api/device",
        headers=headers,
        json={"name": name, "type": "default", "additionalInfo": {"gateway": True}},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


@pytest.fixture()
def gateway_info(tb_admin_headers: Dict[str, str], http: requests.Session) -> Generator[dict, None, None]:
    """Create a unique gateway device for a test; always cleaned up."""
    name = f"pytest-gw-device-{uuid.uuid4().hex[:8]}"
    try:
        gw = _create_gateway_device(http, tb_admin_headers, name)
    except HTTPError as e:
        logger.error("Failed to create gateway device %s: %s", name, e)
        pytest.fail("Failed to create or find test gateway device.")
    try:
        yield gw
    finally:
        gw_id = gw["id"]["id"]
        # Attempt to delete gateway at teardown (sub-devices handled by gateway_config fixture if created).
        http.delete(f"{TB_URL}/api/device/{gw_id}", headers=tb_admin_headers, timeout=REQUEST_TIMEOUT)


@pytest.fixture()
def gateway_config(
        gateway_info: dict, tb_admin_headers: Dict[str, str], http: requests.Session
) -> Generator[GatewayConfig, None, None]:
    """Return a GatewayConfig and clean up gateway + any auto-created sub-device/profile."""
    device_id = gateway_info["id"]["id"]
    token = _get_device_credentials_token(http, tb_admin_headers, device_id)

    cfg = GatewayConfig()
    cfg.host = TB_HOST
    cfg.port = TB_MQTT_PORT
    cfg.access_token = token

    # Provide config to the test
    yield cfg

    # Cleanup: delete gateway and any auto-created sub-device/profile if present.
    try:
        subdevice_id: Optional[str] = None
        try:
            subdevice_id = find_related_entity_id(device_id, TB_URL, tb_admin_headers)
        except Exception:
            subdevice_id = None

        http.delete(f"{TB_URL}/api/device/{device_id}", headers=tb_admin_headers, timeout=REQUEST_TIMEOUT)

        if subdevice_id:
            sub_dev = get_device_info_by_id(subdevice_id, TB_URL, tb_admin_headers)
            http.delete(f"{TB_URL}/api/device/{subdevice_id}", headers=tb_admin_headers, timeout=REQUEST_TIMEOUT)
            if "deviceProfileId" in sub_dev and "id" in sub_dev["deviceProfileId"]:
                http.delete(
                    f"{TB_URL}/api/deviceProfile/{sub_dev['deviceProfileId']['id']}",
                    headers=tb_admin_headers,
                    timeout=REQUEST_TIMEOUT,
                )
    except requests.RequestException as e:
        logger.warning("Gateway cleanup encountered an error: %s", e)


# ----------------------------
# Firmware/profile fixtures
# ----------------------------

@pytest.fixture()
def firmware_profile_and_package(
        tb_admin_headers: Dict[str, str],
        test_config: Dict[str, object],
        http: requests.Session,
) -> Generator[tuple[dict, dict, dict], None, None]:
    """
    Creates a device profile and uploads a firmware package.
    Returns (device_profile, firmware, firmware_info).
    """
    firmware_name = "pytest-firmware"
    firmware_version = "1.0.0"
    firmware_bytes = b"Firmware binary data for pytest"

    firmware_info = {
        "name": firmware_name,
        "version": firmware_version,
        "data": firmware_bytes,
    }

    device_profile, firmware = create_device_profile_and_firmware(
        firmware_name=firmware_name,
        firmware_version=firmware_version,
        firmware_bytes=firmware_bytes,
        base_url=test_config["tb_url"],  # type: ignore[arg-type]
        headers=tb_admin_headers,
        http=http,
        timeout=REQUEST_TIMEOUT,
    )

    assert device_profile is not None, "Device profile should be created successfully"
    assert firmware is not None, "Firmware should be created successfully"

    try:
        yield device_profile, firmware, firmware_info
    finally:
        # Cleanup
        http.delete(
            f"{test_config['tb_url']}/api/deviceProfile/{device_profile['id']['id']}",  # type: ignore[index]
            headers=tb_admin_headers,
            timeout=REQUEST_TIMEOUT,
        )
        http.delete(
            f"{test_config['tb_url']}/api/firmware/{firmware['id']['id']}",  # type: ignore[index]
            headers=tb_admin_headers,
            timeout=REQUEST_TIMEOUT,
        )


# ----------------------------
# Rule chain fixtures
# ----------------------------

@pytest.fixture()
def rpc_rule_chain(
        tb_admin_headers: Dict[str, str], test_config: Dict[str, object], http: requests.Session
) -> Generator[dict, None, None]:
    """
    Creates a rule chain based on RPC_RESPONSE_RULE_CHAIN and uploads its metadata.
    Cleans up the rule chain afterwards.
    """
    rule_chain_info = deepcopy(RPC_RESPONSE_RULE_CHAIN["ruleChain"])
    r = http.post(
        f"{test_config['tb_url']}/api/ruleChain",
        headers=tb_admin_headers,
        json=rule_chain_info,
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    rule_chain = r.json()
    rule_chain_id = rule_chain["id"]["id"]

    # Get current metadata, then replace nodes/connections from our template
    meta_resp = http.get(
        f"{test_config['tb_url']}/api/ruleChain/{rule_chain_id}/metadata",
        headers=tb_admin_headers,
        timeout=REQUEST_TIMEOUT,
    )
    meta_resp.raise_for_status()

    received_metadata = meta_resp.json()
    tpl_meta = deepcopy(RPC_RESPONSE_RULE_CHAIN["metadata"])

    received_metadata["nodes"] = tpl_meta["nodes"]
    now_ms = int(time.time() * 1000)
    for node in received_metadata["nodes"]:
        node["createdTime"] = now_ms

    received_metadata["connections"] = tpl_meta["connections"]
    received_metadata["firstNodeIndex"] = tpl_meta["firstNodeIndex"]
    received_metadata["ruleChainId"] = {"id": rule_chain_id, "entityType": "RULE_CHAIN"}

    apply_resp = http.post(
        f"{test_config['tb_url']}/api/ruleChain/metadata",
        headers=tb_admin_headers,
        json=received_metadata,
        timeout=REQUEST_TIMEOUT,
    )
    apply_resp.raise_for_status()

    try:
        yield rule_chain
    finally:
        http.delete(
            f"{test_config['tb_url']}/api/ruleChain/{rule_chain_id}",
            headers=tb_admin_headers,
            timeout=REQUEST_TIMEOUT,
        )


@pytest.fixture()
def device_profile_with_rpc_rule_chain(
        tb_admin_headers: Dict[str, str],
        test_config: Dict[str, object],
        rpc_rule_chain: dict,
        http: requests.Session,
) -> Generator[dict, None, None]:
    """
    Creates a device profile that uses the RPC rule chain as default; cleans up afterward.
    """
    rule_chain = rpc_rule_chain
    device_profile = get_default_device_profile(test_config["tb_url"], tb_admin_headers)
    device_profile["id"] = None
    device_profile["createdTime"] = None
    device_profile["name"] = "pytest-client-side-rpc-profile"
    device_profile["defaultRuleChainId"] = rule_chain["id"]
    device_profile.setdefault(
        "profileData",
        {
            "configuration": {"type": "DEFAULT"},
            "transportConfiguration": {"type": "DEFAULT"},
        },
    )

    r = http.post(
        f"{test_config['tb_url']}/api/deviceProfile",
        headers=tb_admin_headers,
        json=device_profile,
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    created = r.json()
    try:
        yield created
    finally:
        device_profile_id = created["id"]["id"]
        http.delete(
            f"{test_config['tb_url']}/api/deviceProfile/{device_profile_id}",
            headers=tb_admin_headers,
            timeout=REQUEST_TIMEOUT,
        )
