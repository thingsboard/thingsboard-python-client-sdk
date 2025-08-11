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
from collections import OrderedDict
from uuid import UUID

import pytest

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.gateway.event_type import GatewayEventType
from tb_mqtt_client.entities.gateway.gateway_uplink_message import (
    GatewayUplinkMessage,
    GatewayUplinkMessageBuilder
)


@pytest.mark.asyncio
async def test_direct_instantiation_forbidden():
    with pytest.raises(TypeError) as exc:
        GatewayUplinkMessage(device_name="dev", device_profile="prof")
    assert "Direct instantiation" in str(exc.value)


@pytest.mark.asyncio
async def test_repr_and_build_and_properties():
    # Prepare entities
    attr1 = AttributeEntry("a1", "v1")
    attr2 = AttributeEntry("a2", "v2")
    ts1 = TimeseriesEntry("t1", 123)
    ts2 = TimeseriesEntry("t2", 456)

    f1 = asyncio.get_event_loop().create_future()
    f2 = asyncio.get_event_loop().create_future()

    msg = GatewayUplinkMessage.build(
        device_name="dev1",
        device_profile="prof1",
        attributes=[attr1, attr2],
        timeseries=OrderedDict({111: [ts1, ts2]}),
        delivery_futures=[f1, f2],
        size=123,
        main_ts=999,
    )

    # __repr__
    rep = repr(msg)
    assert "dev1" in rep and "prof1" in rep

    # Properties
    assert msg.device_name == "dev1"
    assert msg.device_profile == "prof1"
    assert msg.size == 123
    assert msg.event_type == GatewayEventType.DEVICE_UPLINK

    # Datapoint counts
    assert msg.timeseries_datapoint_count() == 2
    assert msg.attributes_datapoint_count() == 2
    assert msg.has_attributes()
    assert msg.has_timeseries()

    # Futures
    futs = msg.get_delivery_futures()
    assert futs == (f1, f2)

    # set_main_ts
    assert msg.set_main_ts(555).main_ts == 555


@pytest.mark.asyncio
async def test_builder_minimal_and_defaults():
    b = GatewayUplinkMessageBuilder()
    b.set_device_name("devname")
    b.set_device_profile("profile")
    attr = AttributeEntry("akey", "aval")
    ts = TimeseriesEntry("tkey", 1)
    b.add_attributes(attr)
    b.add_timeseries(ts)

    # Delivery future gets added
    fut = asyncio.get_event_loop().create_future()
    b.add_delivery_futures(fut)

    b.set_main_ts(1000)
    msg = b.build()

    assert isinstance(msg, GatewayUplinkMessage)
    assert msg.device_name == "devname"
    assert msg.device_profile == "profile"
    assert msg.main_ts == 1000
    assert msg.has_attributes()
    assert msg.has_timeseries()


@pytest.mark.asyncio
async def test_builder_defaults_when_not_set():
    # No device_profile and no futures -> builder should create defaults
    b = GatewayUplinkMessageBuilder()
    b.set_device_name("d1")

    msg = b.build()
    assert msg.device_profile == "default"
    assert isinstance(msg.get_delivery_futures()[0], asyncio.Future)
    # The auto-created future has uuid
    assert isinstance(msg.get_delivery_futures()[0].uuid, UUID)


@pytest.mark.asyncio
async def test_add_attributes_and_timeseries_variants():
    b = GatewayUplinkMessageBuilder()
    b.set_device_name("dev")
    attr_list = [AttributeEntry("a1", "v1"), AttributeEntry("a2", "v2")]
    ts_list = [TimeseriesEntry("t1", 1), TimeseriesEntry("t2", 2)]

    # Add as list
    b.add_attributes(attr_list)
    b.add_timeseries(ts_list)

    # Add OrderedDict directly
    od = OrderedDict({123: [TimeseriesEntry("k", 42)]})
    b.add_timeseries(od)

    msg = b.build()
    assert msg.has_attributes()
    assert msg.has_timeseries()


@pytest.mark.asyncio
async def test_len_tracks_size():
    b = GatewayUplinkMessageBuilder()
    base_len = len(b)
    b.set_device_name("abc")
    b.set_device_profile("xyz")
    b.add_attributes(AttributeEntry("k", "v"))
    b.add_timeseries(TimeseriesEntry("ts", 1))
    assert len(b) > base_len


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
