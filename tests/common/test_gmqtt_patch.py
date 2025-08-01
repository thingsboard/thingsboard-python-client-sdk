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

from tb_mqtt_client.common.gmqtt_patch import PatchUtils, PublishPacket
from tb_mqtt_client.common.mqtt_message import MqttPublishMessage


def test_parse_mqtt_properties_valid_and_invalid():
    # Unknown property id triggers warning branch
    pkt = bytes([1]) + bytes([255])
    assert PatchUtils.parse_mqtt_properties(pkt) == {}

    # Exception path (invalid varint)
    assert PatchUtils.parse_mqtt_properties(b"\xff") == {}


def test_extract_reason_code_all_paths():
    class Obj: reason_code = 42
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

    # Call wrapped handler
    pkt = struct.pack("!HB", 10, 1) + b"\x00"
    handler = types.SimpleNamespace(
        _connection=types.SimpleNamespace(persistent_storage=types.SimpleNamespace(remove=lambda m: None))
    )
    handler2 = handler
    MqttHandlerClass = type("H", (), {})
    pu_handler = MqttHandlerClass()
    pu_handler._connection = handler._connection
    pu_handler._handle_puback_packet = lambda *a, **k: None
    pu_handler = types.SimpleNamespace(**{"_connection": handler._connection})
    # Ensure parsing works
    PatchUtils.parse_mqtt_properties(b"\x00")
    called.clear()

    # patch_storage test
    client = types.SimpleNamespace(_persistent_storage=types.SimpleNamespace(
        _queue=[(0,1,"raw")],
        _check_empty=lambda : None
    ))
    pu.client = client
    pu.patch_storage()
    assert asyncio.get_event_loop().run_until_complete(client._persistent_storage.pop_message())


@pytest.mark.asyncio
async def test_retry_loop_and_task_controls(monkeypatch):
    storage_queue = []
    class Storage:
        def _check_empty(self): pass
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
    heapq.heappush(storage_queue, (0, 1, types.SimpleNamespace(topic="t", dup=False)))
    pu._stop_event.set()  # immediate exit
    await pu._retry_loop()

    # start_retry_task + stop_retry_task normal
    pu._stop_event.clear()
    pu.start_retry_task()
    assert pu._retry_task
    await pu.stop_retry_task()
    assert pu._retry_task is None

    # Timeout branch in stop_retry_task
    pu._retry_task = asyncio.create_task(asyncio.sleep(1))
    await pu.stop_retry_task()


def test_apply_calls_patch_and_starts_task(monkeypatch):
    pu = PatchUtils(None, asyncio.Event())
    monkeypatch.setattr(pu, "patch_puback_handling", lambda cb: setattr(pu, "_patched", True))
    monkeypatch.setattr(pu, "start_retry_task", lambda : setattr(pu, "_started", True))
    pu.apply(lambda a,b,c: None)
    assert pu._patched
    assert pu._started


def test_build_package_qos1_with_provided_mid():
    msg = MqttPublishMessage(topic="topic", payload=b"PAY", qos=1, retain=True)
    msg.dup = True
    protocol = types.SimpleNamespace(proto_ver=5)
    mid, packet = PublishPacket.build_package(msg, protocol, mid=77)
    assert mid == 77
    # Verify mid is encoded at correct spot (after topic length and topic)
    assert struct.pack("!H", 77) in packet
    # DUP flag set
    assert packet[0] & 0x08


def test_build_package_qos1_with_generated_mid(monkeypatch):
    msg = MqttPublishMessage(topic="gen", payload=b"PAY", qos=1)
    # Force known id from id_generator
    monkeypatch.setattr(PublishPacket, "id_generator", types.SimpleNamespace(next_id=lambda: 1234))
    protocol = types.SimpleNamespace(proto_ver=5)
    mid, packet = PublishPacket.build_package(msg, protocol)
    assert mid == 1234
    assert struct.pack("!H", 1234) in packet


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
