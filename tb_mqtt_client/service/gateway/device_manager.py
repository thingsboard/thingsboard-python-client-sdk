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

from typing import Optional, Dict, Iterable, Callable, Set
from uuid import UUID

from tb_mqtt_client.common.logging_utils import get_logger
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.entities.gateway.device_info import DeviceInfo


logger = get_logger(__name__)


class DeviceManager:
    def __init__(self):
        self._sessions_by_id: Dict[UUID, DeviceSession] = {}
        self._ids_by_device_name: Dict[str, UUID] = {}
        self._ids_by_original_name: Dict[str, UUID] = {}
        self.__connected_devices: Set[DeviceSession] = set()


    def register(self, device_name: str, device_profile: str = "default") -> DeviceSession:
        session = self.get_by_name(device_name)
        if session:
            return session

        device_info = DeviceInfo(
            device_name=device_name,
            device_profile=device_profile
        )
        session = DeviceSession(device_info, self.__state_change_callback)
        self._sessions_by_id[device_info.device_id] = session
        self._ids_by_device_name[device_name] = device_info.device_id
        self._ids_by_original_name[device_info.original_name] = device_info.device_id
        session.update_last_seen()
        return session

    def unregister(self, device_id: UUID):
        session = self._sessions_by_id.pop(device_id, None)
        if session:
            self._ids_by_device_name.pop(session.device_info.device_name, None)
            self._ids_by_original_name.pop(session.device_info.original_name, None)

    def get_by_id(self, device_id: UUID) -> Optional[DeviceSession]:
        return self._sessions_by_id.get(device_id)

    def get_by_name(self, device_name: str) -> Optional[DeviceSession]:
        device_id = self._ids_by_device_name.get(device_name)
        if device_id:
            return self._sessions_by_id.get(device_id)
        renamed_id = self._ids_by_original_name.get(device_name)
        if renamed_id:
            return self._sessions_by_id.get(renamed_id)
        return None

    def is_connected(self, device_id: UUID) -> bool:
        return device_id in self._sessions_by_id

    def all(self) -> Iterable[DeviceSession]:
        return self._sessions_by_id.values()

    def rename_device(self, old_name: str, new_name: str):
        device_session = self.get_by_name(old_name)
        if not device_session:
            logger.warning(f"Device with name '{old_name}' not found for renaming to '{new_name}'.")
            return
        device_session.device_info.rename(new_name)
        self._ids_by_device_name.pop(old_name, None)
        device_id = device_session.device_info.device_id
        self._ids_by_device_name[new_name] = device_id
        self._ids_by_original_name[device_session.device_info.original_name] = device_id

    def set_attribute_update_callback(self, device_id: UUID, cb: Callable):
        session = self._sessions_by_id.get(device_id)
        if session:
            session.set_attribute_update_callback(cb)

    def set_attribute_response_callback(self, device_id: UUID, cb: Callable):
        session = self._sessions_by_id.get(device_id)
        if session:
            session.set_attribute_response_callback(cb)

    def set_rpc_request_callback(self, device_id: UUID, cb: Callable):
        session = self._sessions_by_id.get(device_id)
        if session:
            session.set_rpc_request_callback(cb)

    def set_rpc_response_callback(self, device_id: UUID, cb: Callable):
        session = self._sessions_by_id.get(device_id)
        if session:
            session.set_rpc_response_callback(cb)

    def __state_change_callback(self, device_session: DeviceSession) -> None:
        if device_session.state.is_connected() and device_session.device_info.device_id not in self.__connected_devices:
            self.__connected_devices.add(device_session)
        else:
            self.__connected_devices.remove(device_session)
        logger.debug(f"Device {device_session.device_info.device_name} state changed to {device_session.state}")

    @property
    def connected_devices(self) -> Set[DeviceSession]:
        return self.__connected_devices

    @property
    def all_devices(self) -> Dict[UUID, DeviceSession]:
        return self._sessions_by_id

    def __repr__(self):
        return f"DeviceManager({str(list(self._ids_by_device_name.keys()))})"
