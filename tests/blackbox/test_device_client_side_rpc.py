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

from examples.device import send_client_side_rpc
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tests.blackbox.rest_helpers import save_device


@pytest.mark.asyncio
@pytest.mark.blackbox
async def test_client_side_rpc(device_profile_with_rpc_rule_chain, device_info, device_config, tb_admin_headers, test_config):
    handler_future = asyncio.Future()
    async def rpc_response_handler(rpc_request):
        handler_future.set_result(rpc_request)

    send_client_side_rpc.config = device_config
    send_client_side_rpc.rpc_response_callback = rpc_response_handler

    device_profile_id = device_profile_with_rpc_rule_chain['id']
    device_info['deviceProfileId'] = device_profile_id
    device = save_device(device_info, test_config['tb_url'], tb_admin_headers)
    assert device is not None, "Device should be saved successfully"

    await asyncio.sleep(1)  # Ensure the device is saved before starting the RPC request

    task = asyncio.create_task(send_client_side_rpc.main())
    result = None
    try:
        result = await asyncio.wait_for(handler_future, timeout=30)
    except Exception as e:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    first_rpc_response = send_client_side_rpc.rpc_response
    second_rpc_response = result

    assert first_rpc_response is not None, "First RPC response should not be None"
    assert isinstance(first_rpc_response, RPCResponse), "First RPC response should be an instance of RPCResponse"
    assert first_rpc_response.request_id < second_rpc_response.request_id, "First RPC response ID should be less than second RPC response ID"
    assert first_rpc_response.result['method'] == 'getTime', "First RPC response method should be 'getTime'"

    assert second_rpc_response is not None, "Second RPC response should not be None"
    assert isinstance(second_rpc_response, RPCResponse), "Second RPC response should be an instance of RPCResponse"
    assert second_rpc_response.result['method'] == 'getStatus', "Second RPC response method should be 'getStatus'"
    assert second_rpc_response.result['params'] == {'param1': 'value1', 'param2': 'value2'}, "Second RPC response params should match expected values"
