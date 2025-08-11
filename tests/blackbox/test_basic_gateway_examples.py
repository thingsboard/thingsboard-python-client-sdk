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

from examples.gateway import connect_and_disconnect_device
from examples.gateway import send_timeseries
from examples.gateway import send_attributes
from examples.gateway import request_attributes
from examples.gateway import handle_attribute_updates
from examples.gateway import handle_rpc_requests
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.gateway.gateway_requested_attribute_response import GatewayRequestedAttributeResponse
from tb_mqtt_client.entities.gateway.gateway_rpc_request import GatewayRPCRequest
from tb_mqtt_client.entities.gateway.gateway_rpc_response import GatewayRPCResponse
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tests.blackbox.rest_helpers import get_device_info_by_name, get_device_attributes, get_device_timeseries, \
    update_shared_attributes, send_rpc_request


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_gateway_connect_and_disconnect_device(gateway_config, tb_admin_headers, test_config):
    device_name = "pytest-gw-subdevice"
    device_profile = "pytest-gw-subdevice-profile"
    connect_and_disconnect_device.config = gateway_config
    connect_and_disconnect_device.device_name = device_name
    connect_and_disconnect_device.device_profile = device_profile

    task = asyncio.create_task(connect_and_disconnect_device.main())

    try:
        await asyncio.sleep(3)
        device_info = get_device_info_by_name(device_name, test_config["tb_url"], tb_admin_headers)
        device_service_attributes = get_device_attributes(device_info["id"]["id"], test_config["tb_url"], tb_admin_headers, "SERVER_SCOPE")
        server_attributes = {attr["key"]: attr["value"] for attr in device_service_attributes}
        assert device_info['type'] == device_profile
        assert server_attributes['active']
        assert abs(server_attributes['lastActivityTime'] - server_attributes["lastConnectTime"]) < 5
        assert server_attributes['lastDisconnectTime'] >= server_attributes["lastConnectTime"]
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_gateway_send_timeseries(gateway_config, tb_admin_headers, test_config):
    device_name = "pytest-gw-subdevice-ts"
    device_profile = "pytest-gw-subdevice-ts-profile"

    send_timeseries.config = gateway_config
    send_timeseries.device_name = device_name
    send_timeseries.device_profile = device_profile

    task = asyncio.create_task(send_timeseries.main())

    try:
        await asyncio.sleep(5)

        device_info = get_device_info_by_name(
            device_name, test_config["tb_url"], tb_admin_headers
        )
        assert device_info is not None, "Subdevice was not created/connected."

        timeseries = get_device_timeseries(
            device_info["id"]["id"], test_config["tb_url"], tb_admin_headers
        )

        expected_keys = {"temperature", "humidity"}
        for key in expected_keys:
            assert key in timeseries, f"Timeseries key '{key}' not found in device data."

        for key in expected_keys:
            assert any(isinstance(entry.get("value"), (int, float)) for entry in timeseries[key]), \
                f"No numeric values found for '{key}'."

    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_gateway_send_attributes(gateway_config, tb_admin_headers, test_config):
    device_name = "pytest-gw-subdevice-attrs"
    device_profile = "pytest-gw-subdevice-attrs-profile"

    send_attributes.config = gateway_config
    send_attributes.device_name = device_name
    send_attributes.device_profile = device_profile

    task = asyncio.create_task(send_attributes.main())

    try:
        await asyncio.sleep(5)

        device_info = get_device_info_by_name(
            device_name, test_config["tb_url"], tb_admin_headers
        )
        assert device_info is not None, "Subdevice was not created/connected."

        attrs = get_device_attributes(
            device_info["id"]["id"],
            test_config["tb_url"],
            tb_admin_headers,
            scope="CLIENT_SCOPE"
        )
        attr_dict = {a["key"]: a["value"] for a in attrs}

        expected_attrs = {
            "maintenance": "scheduled",
            "id": 341,
            "location": "office",
            "status": "active",
            "version": "1.0.0"
        }

        for key, value in expected_attrs.items():
            assert key in attr_dict, f"Attribute '{key}' not found."
            assert attr_dict[key] == value, (
                f"Attribute '{key}' expected '{value}', got '{attr_dict[key]}'"
            )

    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_gateway_request_attributes(gateway_config, tb_admin_headers, test_config):
    device_name = "pytest-gw-subdevice-attrs-request"
    device_profile = "pytest-gw-subdevice-attrs-request-profile"

    expected_attrs = {
        "maintenance": "scheduled",
        "id": 341,
        "location": "office",
    }

    handler_future = asyncio.Future()

    async def requested_attributes_handler(device_session, response: GatewayRequestedAttributeResponse):
        try:
            assert device_session is not None, "Device session should not be None."
            assert device_session.device_info is not None, "Device info should not be None."
            assert device_session.device_info.device_name == device_name, (
                f"Device name mismatch: expected '{device_name}', got '{device_session.device_info.device_name}'"
            )
            assert response is not None, "Response should not be None."
            assert isinstance(response.client, list), "Client attributes should be a list."
            assert isinstance(response.shared, list), "Shared attributes should be a list."
            if response.request_id == 1:  # Assuming request_id 1 is for the client attributes
                assert len(response.client) > 0, "Client attributes should not be empty."
                assert len(response.shared) >= 0, "Shared attributes can be empty."
                for attr in response.client:
                    for key, value in expected_attrs.items():
                        if attr.key == key:
                            assert attr.value == value, (
                                f"Attribute '{key}' expected '{value}', got '{attr.value}'"
                            )
            else:
                assert response.client == [], "Client attributes should be empty for shared attribute requests."
                assert response.shared == [], "Shared attributes should be empty, because they were not set."
            handler_future.set_result(True)
        except AssertionError as e:
            if not handler_future.done():
                handler_future.set_exception(e)

    request_attributes.config = gateway_config
    request_attributes.device_name = device_name
    request_attributes.device_profile = device_profile
    request_attributes.requested_attributes_handler = requested_attributes_handler

    task = asyncio.create_task(request_attributes.main())

    try:
        await asyncio.wait_for(handler_future, timeout=10)

        device_info = get_device_info_by_name(device_name, test_config["tb_url"], tb_admin_headers)
        assert device_info is not None, "Subdevice was not created/connected."

        attrs = get_device_attributes(
            device_info["id"]["id"],
            test_config["tb_url"],
            tb_admin_headers,
            scope="CLIENT_SCOPE"
        )
        attr_dict = {a["key"]: a["value"] for a in attrs}

        for key, value in expected_attrs.items():
            assert key in attr_dict, f"Attribute '{key}' not found."
            assert attr_dict[key] == value, (
                f"Attribute '{key}' expected '{value}', got '{attr_dict[key]}'"
            )

    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_gateway_handle_attribute_updates(gateway_config, tb_admin_headers, test_config):
    device_name = "pytest-gw-subdevice-attrs-update"
    device_profile = "pytest-gw-subdevice-attrs-update-profile"

    test_shared_attribute = {"pytest_shared_attribute": "test_value"}

    handler_future = asyncio.Future()

    async def attribute_update_handler(device_session: DeviceSession, update: AttributeUpdate):
        try:
            assert device_session is not None, "Device session should not be None."
            assert device_session.device_info is not None, "Device info should not be None."
            assert device_session.device_info.device_name == device_name, (
                f"Device name mismatch: expected '{device_name}', got '{device_session.device_info.device_name}'"
            )
            assert update is not None, "Update should not be None."
            assert update.keys()[0] == list(test_shared_attribute.keys())[0], (
                "Update key does not match expected key."
            )
            assert update.values()[0] == list(test_shared_attribute.values())[0], (
                "Update value does not match expected value."
            )
            handler_future.set_result(True)
        except AssertionError as e:
            if not handler_future.done():
                handler_future.set_exception(e)

    handle_attribute_updates.config = gateway_config
    handle_attribute_updates.device_name = device_name
    handle_attribute_updates.device_profile = device_profile
    handle_attribute_updates.attribute_update_handler = attribute_update_handler

    task = asyncio.create_task(handle_attribute_updates.main())

    try:
        await asyncio.sleep(3)

        sub_device_info = get_device_info_by_name(device_name, test_config["tb_url"], tb_admin_headers)

        update_shared_attributes(
            sub_device_info["id"]["id"],
            test_config["tb_url"],
            tb_admin_headers,
            test_shared_attribute
        )

        await asyncio.wait_for(handler_future, timeout=10)

    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_gateway_handle_rpc_requests(gateway_config, tb_admin_headers, test_config):
    device_name = "pytest-gw-subdevice-rpc"
    device_profile = "pytest-gw-subdevice-rpc-profile"

    handler_future = asyncio.Future()

    async def device_rpc_request_handler(device_session: DeviceSession, rpc_request: GatewayRPCRequest):
        try:
            assert device_session is not None, "Device session should not be None."
            assert device_session.device_info is not None, "Device info should not be None."
            assert device_session.device_info.device_name == device_name, (
                f"Device name mismatch: expected '{device_name}', got '{device_session.device_info.device_name}'"
            )
            assert rpc_request is not None, "RPC request should not be None."
            assert rpc_request.method == "testMethod", "RPC method does not match expected method."
            response_data = {
                "data": {
                    "device_name": device_session.device_info.device_name,
                    "request_id": rpc_request.request_id,
                    "method": rpc_request.method,
                    "params": rpc_request.params
                }
            }
            rpc_response = GatewayRPCResponse.build(
                device_session.device_info.device_name,
                rpc_request.request_id,
                response_data
            )
            handler_future.set_result(True)
            return rpc_response
        except AssertionError as e:
            if not handler_future.done():
                handler_future.set_exception(e)

    handle_rpc_requests.config = gateway_config
    handle_rpc_requests.device_name = device_name
    handle_rpc_requests.device_profile = device_profile
    handle_rpc_requests.device_rpc_request_handler = device_rpc_request_handler

    task = asyncio.create_task(handle_rpc_requests.main())
    await asyncio.sleep(3)

    try:

        sub_device_info = get_device_info_by_name(device_name, test_config["tb_url"], tb_admin_headers)
        rpc_request = {
            "method": "testMethod",
            "params": {"param1": "value1"}
        }

        response_task = asyncio.create_task(send_rpc_request(sub_device_info['id']['id'], test_config['tb_url'], tb_admin_headers, rpc_request))

        await asyncio.wait_for(handler_future, timeout=10)

        response_data = await asyncio.wait_for(response_task, timeout=10)

        assert 'result' in response_data, "RPC response should contain 'result'."
        assert 'data' in response_data['result'], "RPC response should contain 'data'."
        assert 'device_name' in response_data['result']['data'], "RPC response data should contain 'device_name'."
        assert response_data['result']['data']['device_name'] == device_name, "RPC response device_name does not match expected device_name."
        assert 'method' in response_data['result']['data'], "RPC response data should contain 'method'."
        assert response_data['result']['data']['method'] == "testMethod", "RPC response method does not match expected method."
        assert 'request_id' in response_data['result']['data'], "RPC response data should contain 'request_id'."
        assert response_data['result']['data']['request_id'] == 0, "RPC response request_id does not match expected request_id."
        assert 'params' in response_data['result']['data'], "RPC response data should contain 'params'."
        assert response_data['result']['data']['params'] == {"param1": "value1"}, "RPC response params do not match expected params."

    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
