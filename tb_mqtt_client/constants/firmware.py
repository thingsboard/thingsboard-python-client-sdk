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

from enum import Enum


class FirmwareStates(Enum):
    IDLE = 'IDLE'
    DOWNLOADING = 'DOWNLOADING'
    DOWNLOADED = 'DOWNLOADED'
    VERIFIED = 'VERIFIED'
    FAILED = 'FAILED'
    UPDATING = 'UPDATING'
    UPDATED = 'UPDATED'


FW_TITLE_ATTR = "fw_title"
FW_VERSION_ATTR = "fw_version"
FW_CHECKSUM_ATTR = "fw_checksum"
FW_CHECKSUM_ALG_ATTR = "fw_checksum_algorithm"
FW_SIZE_ATTR = "fw_size"
FW_STATE_ATTR = "fw_state"

REQUIRED_SHARED_KEYS = [FW_CHECKSUM_ATTR, FW_CHECKSUM_ALG_ATTR,
                        FW_SIZE_ATTR, FW_TITLE_ATTR, FW_VERSION_ATTR]
