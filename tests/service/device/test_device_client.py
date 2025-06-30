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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.publish_result import PublishResult
from tb_mqtt_client.constants.mqtt_topics import DEVICE_CLAIM_TOPIC
from tb_mqtt_client.entities.data.claim_request import ClaimRequest
from tb_mqtt_client.entities.data.provisioning_request import ProvisioningRequest, AccessTokenProvisioningCredentials
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.service.message_queue import MessageQueue


@pytest.mark.asyncio
async def test_send_timeseries_with_dict():
    client = DeviceClient()
    client._message_queue = AsyncMock(spec=MessageQueue)
    future = asyncio.Future()
    future.set_result(PublishResult("topic", 1, 1, 100, 1))
    client._message_queue.publish.return_value = [future]
    result = await client.send_timeseries({"temp": 22})
    assert isinstance(result, PublishResult)
    assert result.message_id == 1


@pytest.mark.asyncio
async def test_send_timeseries_timeout():
    client = DeviceClient()
    client._message_queue = AsyncMock()
    future = asyncio.Future()
    client._message_queue.publish.return_value = [future]
    result = await client.send_timeseries({"temp": 22}, timeout=0.01)
    assert isinstance(result, PublishResult)
    assert result.message_id == -1


@pytest.mark.asyncio
async def test_send_attributes_dict():
    client = DeviceClient()
    client._message_queue = AsyncMock()
    fut = asyncio.Future()
    fut.set_result(PublishResult("attr", 1, 2, 50, 1))
    client._message_queue.publish.return_value = [fut]
    result = await client.send_attributes({"key": "val"})
    assert isinstance(result, PublishResult)
    assert result.message_id == 2


@pytest.mark.asyncio
async def test_send_rpc_request_timeout():
    client = DeviceClient()
    from tb_mqtt_client.entities.data.rpc_request import RPCRequest
    request = await RPCRequest.build(method="get", params={})
    client._rpc_response_handler.register_request = MagicMock(return_value=asyncio.Future())
    client._message_queue = AsyncMock()
    client._message_queue.publish.return_value = []
    with pytest.raises(TimeoutError):
        await client.send_rpc_request(request, wait_for_publish=True, timeout=0.01)


@pytest.mark.asyncio
async def test_send_rpc_response():
    client = DeviceClient()
    from tb_mqtt_client.entities.data.rpc_response import RPCResponse
    response = RPCResponse.build(123, result={"ok": True})
    client._message_queue = AsyncMock()
    await client.send_rpc_response(response)
    client._message_queue.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_claim_device_success():
    client = DeviceClient()
    client._message_queue = AsyncMock()

    claim = ClaimRequest.build(secret_key="abc")

    fut = asyncio.Future()
    fut.set_result(PublishResult(topic=DEVICE_CLAIM_TOPIC, qos=1, message_id=1, payload_size=12, reason_code=0))
    client._message_queue.publish.return_value = [fut]

    result = await client.claim_device(claim)
    assert isinstance(result, PublishResult)
    assert result.topic == DEVICE_CLAIM_TOPIC


@pytest.mark.asyncio
async def test_claim_device_timeout():
    client = DeviceClient()
    client._message_queue = AsyncMock()

    claim = ClaimRequest.build(secret_key="abc")
    fut = asyncio.Future()
    client._message_queue.publish.return_value = [fut]

    result = await client.claim_device(claim, timeout=0.01)
    assert isinstance(result, PublishResult)
    assert result.message_id == -1



@pytest.mark.asyncio
async def test_claim_device_payload_contains_secret_key():
    client = DeviceClient()
    client._message_queue = AsyncMock()

    claim = ClaimRequest.build(secret_key="my-secret")
    fut = asyncio.Future()
    fut.set_result(PublishResult(topic=DEVICE_CLAIM_TOPIC, qos=1, message_id=3, payload_size=15, reason_code=0))
    client._message_queue.publish.return_value = [fut]

    await client.claim_device(claim)

    client._message_queue.publish.assert_awaited_once()
    args, kwargs = client._message_queue.publish.call_args
    assert kwargs['topic'] == DEVICE_CLAIM_TOPIC
    assert b"my-secret" in kwargs['payload']


@pytest.mark.asyncio
async def test_send_attribute_request():
    client = DeviceClient()
    from tb_mqtt_client.entities.data.attribute_request import AttributeRequest
    request = await AttributeRequest.build(client_keys=["key1"])
    client._requested_attribute_response_handler.register_request = AsyncMock()
    client._message_queue = AsyncMock()
    await client.send_attribute_request(request, AsyncMock())
    client._message_queue.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_rpc_call_timeout():
    client = DeviceClient()
    client._mqtt_manager.publish = AsyncMock()
    client._rpc_response_handler.register_request = MagicMock(return_value=asyncio.Future())
    with pytest.raises(TimeoutError):
        await client.send_rpc_call("reboot", timeout=0.01)


@pytest.mark.asyncio
async def test_disconnect():
    client = DeviceClient()
    client._mqtt_manager.disconnect = AsyncMock()
    await client.disconnect()
    client._mqtt_manager.disconnect.assert_awaited()


@pytest.mark.asyncio
async def test_stop_disconnects_and_shuts_down_queue():
    client = DeviceClient()
    client._mqtt_manager.is_connected = lambda: True
    client._mqtt_manager.disconnect = AsyncMock()
    client._message_queue = AsyncMock()
    await client.stop()
    client._message_queue.shutdown.assert_awaited()
    client._mqtt_manager.disconnect.assert_awaited()


@pytest.mark.asyncio
async def test_handle_rate_limit_invalid_response():
    client = DeviceClient()
    from tb_mqtt_client.entities.data.rpc_response import RPCResponse
    bad_resp = RPCResponse.build(1, result="invalid")
    result = await client._handle_rate_limit_response(bad_resp)
    assert result is None


@pytest.mark.asyncio
async def test_handle_rate_limit_valid_response():
    client = DeviceClient()
    from tb_mqtt_client.entities.data.rpc_response import RPCResponse
    client._mqtt_manager.set_rate_limits = MagicMock()
    payload = {
        "rateLimits": {
            "messages": "10:1,",
            "telemetryMessages": "100:60,",
            "telemetryDataPoints": "500:60,"
        },
        "maxInflightMessages": 200,
        "maxPayloadSize": 512
    }
    resp = RPCResponse.build(1, result=payload)
    result = await client._handle_rate_limit_response(resp)
    assert result is True
    assert client.max_payload_size <= 512


@pytest.mark.asyncio
async def test_provision_success(monkeypatch):
    from tb_mqtt_client.service.device import client as client_module
    mock_prov = AsyncMock()
    mock_prov.provision.return_value = "creds"
    monkeypatch.setattr(client_module, "ProvisioningClient", lambda **kwargs: mock_prov)
    credentials = AccessTokenProvisioningCredentials("provision_device_key", "provision_device_secret")
    req = ProvisioningRequest(host="localhost", credentials=credentials, port=1883, device_name="dev")
    res = await DeviceClient.provision(req)
    assert res == "creds"


@pytest.mark.asyncio
async def test_connects_to_platform_with_tls_using_device_config():
    config = DeviceConfig()
    config.use_tls = MagicMock(return_value=True)
    config.ca_cert = "path/to/ca_cert"
    config.client_cert = "path/to/client_cert"
    config.private_key = "path/to/private_key"

    mqtt_manager = AsyncMock()
    client = DeviceClient(config=config)
    client._mqtt_manager = mqtt_manager
    client._mqtt_manager.connect = AsyncMock()
    client._mqtt_manager.is_connected = MagicMock(return_value=True)
    client._mqtt_manager.await_ready = AsyncMock()

    with patch("tb_mqtt_client.service.device.client.ssl.create_default_context") as mock_ssl_context:
        ssl_context = mock_ssl_context.return_value
        ssl_context.load_verify_locations = MagicMock()
        ssl_context.load_cert_chain = MagicMock()
        await client.connect()

    mock_ssl_context.assert_called_once()
    ssl_context.load_verify_locations.assert_called_once_with(config.ca_cert)
    ssl_context.load_cert_chain.assert_called_once_with(certfile=config.client_cert, keyfile=config.private_key)
    client._mqtt_manager.connect.assert_awaited_once_with(
        host=config.host,
        port=config.port,
        username=config.access_token or config.username,
        password=None if config.access_token else config.password,
        tls=True,
        ssl_context=ssl_context)


@pytest.mark.asyncio
async def test_connects_to_platform_with_tls():
    config = DeviceConfig()
    config.use_tls = MagicMock(return_value=True)
    config.ca_cert = "path/to/ca_cert"
    config.client_cert = "path/to/client_cert"
    config.private_key = "path/to/private_key"
    config.host = "localhost"
    config.port = 8883
    config.access_token = "token"
    config.username = None
    config.password = None

    mqtt_manager = AsyncMock()
    client = DeviceClient(config)
    client._mqtt_manager = mqtt_manager

    with patch("tb_mqtt_client.service.device.client.ssl.create_default_context") as mock_ssl_context:
        ssl_context = mock_ssl_context.return_value
        await client.connect()

    mock_ssl_context.assert_called_once()
    ssl_context.load_verify_locations.assert_called_once_with(config.ca_cert)
    ssl_context.load_cert_chain.assert_called_once_with(
        certfile=config.client_cert,
        keyfile=config.private_key
    )
    mqtt_manager.connect.assert_awaited_once_with(
        host=config.host,
        port=config.port,
        username=config.access_token,
        password=None,
        tls=True,
        ssl_context=ssl_context
    )


@pytest.mark.asyncio
async def test_connects_to_platform_without_tls():
    config = DeviceConfig()
    config.use_tls = MagicMock(return_value=False)
    config.host = "localhost"
    config.port = 1883
    config.access_token = "token"
    config.username = None
    config.password = None

    mqtt_manager = AsyncMock()
    client = DeviceClient(config)
    client._mqtt_manager = mqtt_manager

    await client.connect()

    mqtt_manager.connect.assert_awaited_once_with(
        host=config.host,
        port=config.port,
        username=config.access_token,
        password=None,
        tls=False,
        ssl_context=None
    )


@pytest.mark.asyncio
async def test_stops_if_event_is_set_during_connection():
    config = DeviceConfig()
    config.host = "localhost"
    config.port = 1883
    config.access_token = "token"
    config.username = None
    config.password = None

    mqtt_manager = AsyncMock()
    mqtt_manager.is_connected.return_value = False
    mqtt_manager.await_ready = AsyncMock()

    client = DeviceClient(config)
    client._mqtt_manager = mqtt_manager
    client._stop_event.set()

    await client.connect()

    mqtt_manager.await_ready.assert_not_awaited()
    mqtt_manager.is_connected.assert_not_awaited()


@pytest.mark.asyncio
async def test_initializes_dispatcher_and_queue_after_connection():
    config = DeviceConfig()
    config.host = "localhost"
    config.port = 1883
    config.access_token = "token"
    config.username = None
    config.password = None

    mqtt_manager = AsyncMock()
    mqtt_manager.is_connected.return_value = True

    with patch("tb_mqtt_client.service.device.client.MessageQueue") as mock_queue:
        client = DeviceClient(config)
        client._mqtt_manager = mqtt_manager

        await client.connect()

        assert client.max_payload_size == 65535
        assert client._message_dispatcher is not None
        assert client._message_queue is not None

        mock_queue.assert_called_once()
        kwargs = mock_queue.call_args.kwargs
        assert kwargs["max_queue_size"] == client._max_uplink_message_queue_size


class FakeSplitter:
    def __init__(self):
        self.max_payload_size = None


@pytest.mark.asyncio
async def test_uses_default_max_payload_size_when_not_provided():
    client = DeviceClient()
    client.max_payload_size = None

    splitter = FakeSplitter()
    client._message_dispatcher = MagicMock()
    client._message_dispatcher.splitter = splitter

    resp = RPCResponse.build(1, result={"rateLimits": {}})
    await client._handle_rate_limit_response(resp)

    assert client.max_payload_size == 65535
    assert splitter.max_payload_size == 65535


@pytest.mark.asyncio
async def test_does_not_update_dispatcher_when_not_initialized():
    client = DeviceClient()
    client.max_payload_size = None
    client._message_dispatcher = None

    resp = RPCResponse.build(1, result={"rateLimits": {}})
    await client._handle_rate_limit_response(resp)

    assert client.max_payload_size == 65535


@pytest.mark.asyncio
async def test_send_timeseries_without_connect_raises_error():
    client = DeviceClient()
    with pytest.raises(AttributeError):
        await client.send_timeseries({"temp": 22})


@pytest.mark.asyncio
async def test_handle_attribute_update_calls_handler():
    client = DeviceClient()
    mock_handler = AsyncMock()
    client._attribute_updates_handler.handle = mock_handler
    await client._handle_attribute_update("topic", b'{"key":"value"}')
    mock_handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_rpc_request_triggers_rpc_response():
    client = DeviceClient()
    mock_handler = AsyncMock(return_value=RPCResponse.build(1, result={"res": "ok"}))
    client._rpc_requests_handler.handle = mock_handler
    client.send_rpc_response = AsyncMock()
    await client._handle_rpc_request("topic", b'{"method": "test"}')
    client.send_rpc_response.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_rate_limit_response_with_partial_data():
    client = DeviceClient()
    response = RPCResponse.build(1, result={"rateLimits": {"messages": "5:1"}})
    result = await client._handle_rate_limit_response(response)
    assert result is True
    assert client._messages_rate_limit.has_limit()
    assert client.max_payload_size == 65535


@pytest.mark.asyncio
async def test_update_firmware_triggers_firmware_updater():
    client = DeviceClient()
    updater = AsyncMock()
    client._firmware_updater = updater
    await client.update_firmware()
    updater.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_rpc_request_with_callback_executes():
    client = DeviceClient()
    rpc = await RPCRequest.build(method="get", params={})
    client._rpc_response_handler.register_request = MagicMock(return_value=AsyncMock())
    client._message_queue = AsyncMock()
    client._message_queue.publish.return_value = []

    callback = AsyncMock()
    await client.send_rpc_request(rpc, callback=callback, wait_for_publish=False)
    client._rpc_response_handler.register_request.assert_called_once()


@pytest.mark.asyncio
async def test_set_attribute_update_callback_sets_handler():
    client = DeviceClient()
    cb = AsyncMock()
    client.set_attribute_update_callback(cb)
    assert client._attribute_updates_handler._callback == cb


@pytest.mark.asyncio
async def test_send_timeseries_with_invalid_type_raises():
    client = DeviceClient()
    with pytest.raises(ValueError):
        await client.send_timeseries("invalid")


@pytest.mark.asyncio
async def test_send_attributes_no_wait():
    client = DeviceClient()
    client._message_queue = AsyncMock()
    client._message_queue.publish.return_value = [asyncio.Future()]
    result = await client.send_attributes({"attr": "val"}, wait_for_publish=False)
    assert result is None


@pytest.mark.asyncio
async def test_claim_device_returns_future_when_not_waiting():
    client = DeviceClient()
    claim = ClaimRequest.build("secret_key")
    client._message_queue = AsyncMock()
    client._message_queue.publish.return_value = [asyncio.Future()]
    result = await client.claim_device(claim, wait_for_publish=False)
    assert isinstance(result, asyncio.Future)


@pytest.mark.asyncio
async def test_handle_rpc_response_calls_handler():
    client = DeviceClient()
    mock = AsyncMock()
    client._rpc_response_handler.handle = mock
    await client._handle_rpc_response("topic", b"payload")
    mock.assert_awaited_once_with("topic", b"payload")


@pytest.mark.asyncio
async def test_handle_requested_attribute_response_calls_handler():
    client = DeviceClient()
    mock = AsyncMock()
    client._requested_attribute_response_handler.handle = mock
    await client._handle_requested_attribute_response("topic", b"payload")
    mock.assert_awaited_once_with("topic", b"payload")


@pytest.mark.asyncio
async def test_on_disconnect_clears_handlers():
    client = DeviceClient()
    client._requested_attribute_response_handler.clear = MagicMock()
    client._rpc_response_handler.clear = MagicMock()
    await client._on_disconnect()
    client._requested_attribute_response_handler.clear.assert_called_once()
    client._rpc_response_handler.clear.assert_called_once()


@pytest.mark.asyncio
async def test_set_rpc_request_callback_sets_handler():
    client = DeviceClient()
    cb = AsyncMock()
    client.set_rpc_request_callback(cb)
    assert client._rpc_requests_handler._callback == cb


@pytest.mark.asyncio
async def test_provision_timeout(monkeypatch):
    from tb_mqtt_client.service.device import client as client_module
    mock_prov = AsyncMock()
    mock_prov.provision.side_effect = asyncio.TimeoutError()
    monkeypatch.setattr(client_module, "ProvisioningClient", lambda **kwargs: mock_prov)

    credentials = AccessTokenProvisioningCredentials("key", "secret")
    req = ProvisioningRequest(host="localhost", credentials=credentials, port=1883, device_name="dev")
    result = await DeviceClient.provision(req, timeout=0.01)
    assert result is None


if __name__ == '__main__':
    pytest.main([__file__])
