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
import heapq
import struct
import types

import pytest
from gmqtt.mqtt.constants import MQTTv50, MQTTv311, MQTTCommands
from gmqtt.mqtt.handler import MqttPackageHandler
from gmqtt.mqtt.protocol import MQTTProtocol

from tb_mqtt_client.common.gmqtt_patch import PatchUtils, PublishPacket
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage


def test_parse_mqtt_properties_valid_and_invalid():
    pkt = bytes([1]) + bytes([255])
    assert PatchUtils.parse_mqtt_properties(pkt) == {}

    assert PatchUtils.parse_mqtt_properties(b"\xff") == {}


def test_extract_reason_code_all_paths():
    class Obj:
        reason_code = 42

    assert PatchUtils.extract_reason_code(Obj()) == 42
    assert PatchUtils.extract_reason_code(b"\x00*") == 42
    assert PatchUtils.extract_reason_code(b"") is None
    assert PatchUtils.extract_reason_code(None) is None


def test_patch_puback_handling_and_storage(monkeypatch):
    pu = PatchUtils(None, asyncio.Event())
    called = {}

    def on_puback(mid, reason, props):
        called["hit"] = (mid, reason, props)

    monkeypatch.setattr("gmqtt.mqtt.handler.MqttPackageHandler._handle_puback_packet", lambda *a, **k: None)
    pu.patch_puback_handling(on_puback)

    handler = types.SimpleNamespace(
        _connection=types.SimpleNamespace(
            persistent_storage=types.SimpleNamespace(remove=lambda m: None)
        )
    )
    packet = struct.pack("!HB", 0xABCD, 0x10)
    MqttPackageHandler._handle_puback_packet(handler, MQTTCommands.PUBACK, packet)
    assert called["hit"][0] == 0xABCD
    assert called["hit"][1] == 0x10
    assert isinstance(called["hit"][2], dict)

    client = types.SimpleNamespace(
        _persistent_storage=types.SimpleNamespace(
            _queue=[(0.0, 1, "raw")],
            _check_empty=lambda: None
        )
    )
    pu.client = client
    pu.patch_storage()

    tm, mid, raw = asyncio.get_event_loop().run_until_complete(client._persistent_storage.pop_message())
    assert mid == 1 and raw == "raw"


@pytest.mark.asyncio
async def test_retry_loop_and_task_controls(monkeypatch):
    storage_queue = []

    class Storage:
        def _check_empty(self):
            pass

        async def pop_message(self):
            if not storage_queue:
                raise IndexError
            return storage_queue.pop(0)

    msgs_sent = []

    class FakeClient:
        is_connected = True

        async def put_retry_message(self, msg):
            msgs_sent.append(msg)

        _persistent_storage = Storage()

    pu = PatchUtils(FakeClient(), asyncio.Event(), retry_interval=0)
    heapq.heappush(storage_queue, (0.0, 1, types.SimpleNamespace(topic="t", dup=False)))
    pu._stop_event.set()
    await pu._retry_loop()
    assert not msgs_sent
    pu._stop_event.clear()
    pu.start_retry_task()
    assert pu._retry_task is not None
    await pu.stop_retry_task()
    assert pu._retry_task is None

    pu._retry_task = asyncio.create_task(asyncio.sleep(1))
    await pu.stop_retry_task()
    assert pu._retry_task is None


def test_apply_calls_patch_and_starts_task(monkeypatch):
    pu = PatchUtils(None, asyncio.Event())
    monkeypatch.setattr(pu, "patch_puback_handling", lambda cb: setattr(pu, "_patched", True))
    monkeypatch.setattr(pu, "start_retry_task", lambda: setattr(pu, "_started", True))
    pu.apply(lambda a, b, c: None)
    assert pu._patched
    assert pu._started


def test_build_package_qos1_with_provided_mid():
    msg = MqttPublishMessage(topic="topic", payload=b"PAY", qos=1, retain=True)
    msg.dup = True
    protocol = types.SimpleNamespace(proto_ver=5)
    mid, packet = PublishPacket.build_package(msg, protocol, mid=77)
    assert mid == 77
    assert struct.pack("!H", 77) in packet
    assert packet[0] & 0x08


def test_build_package_qos1_with_generated_mid(monkeypatch):
    msg = MqttPublishMessage(topic="gen", payload=b"PAY", qos=1)
    monkeypatch.setattr(PublishPacket, "id_generator", types.SimpleNamespace(next_id=lambda: 1234))
    protocol = types.SimpleNamespace(proto_ver=5)
    mid, packet = PublishPacket.build_package(msg, protocol)
    assert mid == 1234
    assert struct.pack("!H", 1234) in packet


# ------------------------------
# New tests (extended coverage)
# ------------------------------

def test_build_package_qos0_no_mid_empty_payload():
    msg = MqttPublishMessage(topic="t/0", payload=b"", qos=0, retain=False)
    protocol = types.SimpleNamespace(proto_ver=5)
    mid, packet = PublishPacket.build_package(msg, protocol)
    assert mid is None
    assert b"t/0" in packet
    assert (packet[0] >> 1) & 0b11 == 0


@pytest.mark.asyncio
async def test_patch_mqtt_handler_disconnect_invokes_on_disconnect_and_reconnect(monkeypatch):
    assert PatchUtils.patch_mqtt_handler_disconnect(None) is True

    reconnect_called = asyncio.Event()
    on_disconnect_args = {}

    async def fake_reconnect(delay=True):
        reconnect_called.set()

    def fake_on_disconnect(self_like, reason_code, properties, exc):
        on_disconnect_args["rc"] = reason_code
        on_disconnect_args["props"] = properties
        on_disconnect_args["exc"] = exc

    fake_connection = types.SimpleNamespace(_on_disconnect_called=False)
    self_like = types.SimpleNamespace(
        _clear_topics_aliases=lambda: None,
        reconnect=fake_reconnect,
        _handle_exception_in_future=lambda f: None,
        on_disconnect=fake_on_disconnect,
        _connection=fake_connection,
    )

    packet = bytes([151]) + bytes([0])

    MqttPackageHandler._handle_disconnect_packet(self_like, MQTTCommands.DISCONNECT, packet)

    await asyncio.wait_for(reconnect_called.wait(), timeout=1.0)
    assert on_disconnect_args["rc"] == 151
    assert isinstance(on_disconnect_args["props"], dict)
    assert on_disconnect_args["exc"] is None
    assert self_like._connection._on_disconnect_called is True


@pytest.mark.asyncio
async def test_patch_handle_connack_success_and_error_paths(monkeypatch):
    assert PatchUtils.patch_handle_connack(None) is True

    connected_set = {"hit": False}
    on_connect_called = {}

    def _connected_set():
        connected_set["hit"] = True

    success_self = types.SimpleNamespace(
        _connected=types.SimpleNamespace(set=_connected_set),
        _logger=types.SimpleNamespace(warning=lambda *a, **k: None,
                                      info=lambda *a, **k: None,
                                      debug=lambda *a, **k: None),
        failed_connections=0,
        protocol_version=MQTTv50,
        _parse_properties=lambda payload: ({'x': 'y'}, b""),
        _update_keepalive_if_needed=lambda: None,
        properties={},
        on_connect=lambda *args: on_connect_called.setdefault("ok", args),
        disconnect=lambda: asyncio.Future(),
        reconnect=lambda delay=True: asyncio.Future(),
        _handle_exception_in_future=lambda f: None,
    )

    packet_ok = struct.pack("!BB", 1, 0) + b"\x00"
    MqttPackageHandler._handle_connack_packet(success_self, MQTTCommands.CONNACK, packet_ok)
    assert connected_set["hit"] is True
    assert "ok" in on_connect_called

    downgraded = {}

    fut = asyncio.get_event_loop().create_future()
    fut.set_result(None)

    def fake_reconnect(delay=True):
        downgraded["called"] = True
        f = asyncio.get_event_loop().create_future()
        f.set_result(None)
        return f

    error_self = types.SimpleNamespace(
        _connected=types.SimpleNamespace(set=lambda: None),
        _logger=types.SimpleNamespace(warning=lambda *a, **k: None,
                                      info=lambda *a, **k: None,
                                      debug=lambda *a, **k: None),
        failed_connections=0,
        protocol_version=MQTTv50,
        _parse_properties=lambda payload: ({}, b""),
        _update_keepalive_if_needed=lambda: None,
        properties={},
        on_connect=lambda *a, **k: None,
        reconnect=fake_reconnect,
        _handle_exception_in_future=lambda f: None,
    )

    packet_err = struct.pack("!BB", 0, 1)
    MqttPackageHandler._handle_connack_packet(error_self, MQTTCommands.CONNACK, packet_err)
    assert downgraded.get("called") is True
    assert MQTTProtocol.proto_ver == MQTTv311


@pytest.mark.asyncio
async def test_patch_gmqtt_protocol_connection_lost_and_handler_call(monkeypatch):
    assert PatchUtils.patch_gmqtt_protocol_connection_lost(None) is True

    disconnect_pkgs = []

    def put_package(pkg):
        disconnect_pkgs.append(pkg)

    loop = asyncio.get_event_loop()
    read_future = loop.create_future()

    class FakeMQTTProtocol(MQTTProtocol):
        def __init__(self):
            pass

    proto_self = FakeMQTTProtocol()
    proto_self._connected = types.SimpleNamespace(clear=lambda: None)
    proto_self._connection = types.SimpleNamespace(
        _disconnect_exc=None,
        _disconnect_properties=None,
        put_package=put_package
    )
    proto_self._read_loop_future = read_future
    proto_self._queue = None

    proto_self._stream_reader_wr = lambda: types.SimpleNamespace(
        feed_eof=lambda: None,
        set_exception=lambda exc: None
    )
    proto_self._closed = loop.create_future()
    proto_self._paused = False

    proto_self.connection_lost(TimeoutError("simulated"))

    assert disconnect_pkgs and disconnect_pkgs[0][0] == MQTTCommands.DISCONNECT
    payload = disconnect_pkgs[0][1]
    assert isinstance(payload, (bytes, bytearray)) and len(payload) == 1

    reconnect_flag = asyncio.Event()
    on_disc_args = {}

    async def fake_reconnect(delay=True):
        reconnect_flag.set()

    handler_self = types.SimpleNamespace(
        _connection=proto_self._connection,
        _clear_topics_aliases=lambda: None,
        reconnect=fake_reconnect,
        _handle_exception_in_future=lambda f: None,
        on_disconnect=lambda *args: on_disc_args.setdefault("args", args),
    )

    MqttPackageHandler.__call__(handler_self, MQTTCommands.DISCONNECT, b"\x01")
    await asyncio.wait_for(reconnect_flag.wait(), timeout=1.0)
    assert "args" in on_disc_args


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc, expected_reason",
    [
        (ConnectionRefusedError("boom"), 135),  # Keep Alive timeout
        (TimeoutError("boom"), 135),  # Keep Alive timeout
        (ConnectionResetError("boom"), 139),  # Receive Maximum exceeded
        (ConnectionAbortedError("boom"), 136),  # Session taken over
        (PermissionError("boom"), 132),  # Not authorized
        (OSError("boom"), 130),  # Protocol Error
        (ValueError("boom"), 131),  # Implementation specific error (fallback)
    ],
)
async def test_connection_lost_reason_code_mapping(monkeypatch, exc, expected_reason):
    assert PatchUtils.patch_gmqtt_protocol_connection_lost(None) is True

    def build_proto():
        disconnect_pkgs = []

        def put_package(pkg):
            disconnect_pkgs.append(pkg)

        loop = asyncio.get_event_loop()
        read_future = loop.create_future()

        class FakeMQTTProtocol(MQTTProtocol):
            def __init__(self):
                pass

        proto = FakeMQTTProtocol()
        proto._connected = types.SimpleNamespace(clear=lambda: None)
        proto._connection = types.SimpleNamespace(
            _disconnect_exc=None,
            _disconnect_properties=None,
            put_package=put_package
        )
        proto._read_loop_future = read_future
        proto._queue = None
        proto._stream_reader_wr = lambda: types.SimpleNamespace(
            feed_eof=lambda: None,
            set_exception=lambda e: None
        )
        proto._closed = loop.create_future()
        proto._paused = False
        return proto, disconnect_pkgs

    proto_self, disconnect_pkgs = build_proto()

    proto_self.connection_lost(exc)

    assert disconnect_pkgs and disconnect_pkgs[0][0] == MQTTCommands.DISCONNECT
    payload = disconnect_pkgs[0][1]
    assert isinstance(payload, (bytes, bytearray)) and len(payload) == 1
    assert payload[0] == expected_reason

    props = getattr(proto_self._connection, "_disconnect_properties", {})
    assert isinstance(props, dict)
    assert "reason_string" in props
    assert isinstance(props["reason_string"], list) and props["reason_string"][0] == str(exc.args[0])


def test_patch_puback_handling_with_properties(monkeypatch):
    pu = PatchUtils(None, asyncio.Event())
    got = {}

    def on_puback(mid, reason, props):
        got["mid"] = mid
        got["reason"] = reason
        got["props"] = props

    monkeypatch.setattr("gmqtt.mqtt.handler.MqttPackageHandler._handle_puback_packet", lambda *a, **k: None)
    pu.patch_puback_handling(on_puback)

    from gmqtt.mqtt.property import Property
    from gmqtt.mqtt.utils import pack_variable_byte_integer

    reason_prop = Property.factory(name="reason_string").dumps("OK")
    props_len_prefix = pack_variable_byte_integer(len(reason_prop))
    props_payload = bytes(props_len_prefix) + bytes(reason_prop)

    packet = struct.pack("!H", 0x1234) + bytes([0x00]) + props_payload

    handler_self = types.SimpleNamespace(
        _connection=types.SimpleNamespace(
            persistent_storage=types.SimpleNamespace(remove=lambda m: None)
        )
    )

    # Act
    MqttPackageHandler._handle_puback_packet(handler_self, MQTTCommands.PUBACK, packet)

    assert got["mid"] == 0x1234
    assert got["reason"] == 0x00
    assert isinstance(got["props"], dict)
    assert got["props"].get("reason_string") == ["OK"]


def test_parse_properties_reason_string_and_user_property_real():
    from gmqtt.mqtt.property import Property
    from gmqtt.mqtt.utils import pack_variable_byte_integer

    rs = Property.factory(name="reason_string").dumps("OK")
    up = Property.factory(name="user_property").dumps(("k", "v"))
    props = bytes(rs + up)

    packet = bytes(pack_variable_byte_integer(len(props))) + props
    parsed = PatchUtils.parse_mqtt_properties(packet)

    assert parsed["reason_string"] == ["OK"]
    assert parsed["user_property"] == [("k", "v")]


def test_parse_properties_unknown_property_returns_empty():
    packet = b"\x01" + b"\xff"
    assert PatchUtils.parse_mqtt_properties(packet) == {}


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
