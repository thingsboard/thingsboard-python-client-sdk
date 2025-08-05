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

import pytest

from tb_mqtt_client.entities.data.attribute_entry import AttributeEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.gateway.gateway_attribute_update import GatewayAttributeUpdate


def test_init_with_list_of_entries():
    entries = [AttributeEntry("k1", "v1"), AttributeEntry("k2", "v2")]
    obj = GatewayAttributeUpdate("deviceA", entries)
    assert obj.device_name == "deviceA"
    assert len(obj.entries) == 2
    assert all(isinstance(e, AttributeEntry) for e in obj.entries)
    assert isinstance(obj.attribute_update, AttributeUpdate)
    assert str(obj) == f"GatewayAttributeUpdate(device_name=deviceA, attribute_update={obj.attribute_update})"


def test_init_with_single_entry():
    entry = AttributeEntry("k", "v")
    obj = GatewayAttributeUpdate("deviceB", entry)
    assert obj.device_name == "deviceB"
    assert len(obj.entries) == 1
    assert obj.entries[0].key == "k"
    assert isinstance(obj.attribute_update, AttributeUpdate)
    assert "deviceB" in str(obj)


def test_init_with_attribute_update():
    update = AttributeUpdate([AttributeEntry("ka", "va")])
    obj = GatewayAttributeUpdate("deviceC", update)
    assert obj.device_name == "deviceC"
    assert len(obj.entries) == 1
    assert isinstance(obj.attribute_update, AttributeUpdate)
    assert "deviceC" in str(obj)


def test_init_with_invalid_type():
    with pytest.raises(TypeError) as exc:
        GatewayAttributeUpdate("deviceD", {"invalid": "type"})
    assert "attribute_update must be an instance of AttributeUpdate" in str(exc.value)


if __name__ == '__main__':
    pytest.main([__file__, "--tb=short", "-v"])
