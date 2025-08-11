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
from examples.device import send_attributes
from examples.device import send_timeseries
from examples.device import request_attributes
from examples.device import handle_attribute_updates
from examples.device import handle_rpc_requests
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.requested_attribute_response import RequestedAttributeResponse
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tests.blackbox.rest_helpers import get_device_attributes, get_device_timeseries, update_shared_attributes, \
    send_rpc_request


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_send_attributes(device_config, device_info, tb_admin_headers, test_config):
    expected_attributes = {
        "firmwareVersion": "1.0.3",
        "hardwareModel": "TB-SDK-Device",
        "mode": "normal",
        "location": "Building A",
        "status": "active"
    }

    send_attributes.config = device_config

    await send_attributes.main()

    attrs = get_device_attributes(device_info['id']['id'], test_config['tb_url'], tb_admin_headers, 'CLIENT_SCOPE')
    received_keys = {a.get("key") for a in attrs}
    for key, value in expected_attributes.items():
        assert any(a.get("key") == key and a.get("value") == value for a in attrs)
        assert key in received_keys


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_send_timeseries(device_config, device_info, tb_admin_headers, test_config):
    expected_timeseries = {
        "temperature": "any",  # Accept any value for temperature
        "humidity": "any",  # Accept any value for humidity
        "pressure": "any",  # Accept any value for pressure
        "vibration": 0.05,
        "speed": 123
    }
    additional_timeseries = {
        "vibration": 0.01,
        "speed": 120
    }

    send_timeseries.config = device_config
    await send_timeseries.main()

    timeseries = get_device_timeseries(device_info['id']['id'], test_config['tb_url'], tb_admin_headers)
    for key, value in expected_timeseries.items():
        assert key in timeseries, f"Expected timeseries key '{key}' not found."
        assert any((ts.get("value") == value or ts.get("value", True) == additional_timeseries.get(key, False) or value == "any") for ts in timeseries[key]), f"Expected value '{value}' for key '{key}' not found."

    all_timeseries_keys = set(timeseries.keys())
    all_timeseries = get_device_timeseries(device_info['id']['id'], test_config['tb_url'],
                                           tb_admin_headers, keys=list(all_timeseries_keys))
    for key in all_timeseries_keys:
        for ts_entry in all_timeseries[key]:
            assert (expected_timeseries[key] == 'any' or ts_entry.get("value") == expected_timeseries[key] or ts_entry.get("value") == additional_timeseries.get(key)), f"Expected value '{expected_timeseries[key]}' for key '{key}' not found in all timeseries."


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_request_attributes(device_config, device_info, tb_admin_headers, test_config):
    expected_attributes = {
        "currentTemperature": 22.5
    }

    attributes_received = asyncio.Event()

    handler_future = asyncio.Future()

    async def attribute_request_callback(response: RequestedAttributeResponse):
        attributes_received.set()
        try:
            assert response is not None, "Response should not be None"
            assert response.request_id != -1, "Request ID should not be -1"
            assert response.client, "Client attributes should not be empty"
            for expected_attribute in expected_attributes:
                assert any(attr.key == expected_attribute and attr.value == expected_attributes[expected_attribute] for attr in response.client), f"Expected attribute '{expected_attribute}' not found in response"
            handler_future.set_result(True)
        except AssertionError as e:
            handler_future.set_exception(e)

    request_attributes.config = device_config
    request_attributes.attribute_request_callback = attribute_request_callback
    request_attributes.response_received = attributes_received

    await request_attributes.main()

    await asyncio.wait_for(handler_future, timeout=10)

    attrs = get_device_attributes(device_info['id']['id'], test_config['tb_url'], tb_admin_headers, 'CLIENT_SCOPE')
    received_keys = {a.get("key") for a in attrs}
    for key, value in expected_attributes.items():
        assert any(a.get("key") == key and a.get("value") == value for a in attrs)
        assert key in received_keys


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_handle_attribute_updates(device_config, device_info, tb_admin_headers, test_config):

    test_shared_attribute = {"pytest_shared_attribute": "test_value"}

    handler_future = asyncio.Future()

    async def attribute_update_callback(update: AttributeUpdate):
        try:
            assert update is not None, "Update should not be None"
            assert update.keys()[0] == list(test_shared_attribute.keys())[0], "Update key does not match expected key"
            assert update.values()[0] == list(test_shared_attribute.values())[0], "Update value does not match expected value"
            handler_future.set_result(True)
        except AssertionError as e:
            if not handler_future.done():
                handler_future.set_exception(e)

    handle_attribute_updates.attribute_update_callback = attribute_update_callback
    handle_attribute_updates.config = device_config

    task = asyncio.create_task(handle_attribute_updates.main())

    await asyncio.sleep(3)
    try:
        update_shared_attributes(
            device_info['id']['id'],
            test_config['tb_url'],
            tb_admin_headers,
            test_shared_attribute
        )

        await asyncio.wait_for(handler_future, timeout=10)
    except asyncio.TimeoutError:
        pytest.fail("Attribute update was not received within the timeout period")
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_handle_rpc_requests(device_config, device_info, tb_admin_headers, test_config):

    handler_future = asyncio.Future()

    rpc_request = {
        "method": "getDeviceInfo",
        "params": {"deviceId": device_info['id']['id']}
    }

    async def rpc_request_callback(request: RPCRequest):
        try:
            assert request is not None, "RPC request should not be None"
            assert request.method == "getDeviceInfo", "Unexpected RPC method"
            response = RPCResponse.build(request_id=request.request_id,
                                         result={"id": device_info['id'], "name": device_info['name']})
            handler_future.set_result(response)
            return response
        except AssertionError as e:
            if not handler_future.done():
                handler_future.set_exception(e)

    handle_rpc_requests.rpc_request_callback = rpc_request_callback
    handle_rpc_requests.config = device_config

    task = asyncio.create_task(handle_rpc_requests.main())
    await asyncio.sleep(1)

    try:

        response_task = asyncio.create_task(send_rpc_request(device_info['id']['id'], test_config['tb_url'], tb_admin_headers, rpc_request))

        await asyncio.wait_for(handler_future, timeout=10)

        response = await asyncio.wait_for(response_task, timeout=10)

        assert response is not None, "RPC response should not be None"

    except asyncio.TimeoutError:
        pytest.fail("RPC request was not received within the timeout period")
    except Exception as e:
        pytest.fail(f"An unexpected exception occurred: {e}")
    finally:
        task.cancel()