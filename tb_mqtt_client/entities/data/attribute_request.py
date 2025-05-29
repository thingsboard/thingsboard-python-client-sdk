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

from typing import Union


class AttributeRequest:
    """
    Represents a request for attributes, including shared and client attributes.
    This class is used to encapsulate the details of an attribute request.
    If some scope is not needed, it can be set to None.
    shared: list
        A list of shared attribute keys to the request. If empty - all shared attributes will be requested.
    client: list
        A list of client attribute keys to the request. If empty - all client attributes will be requested.
    """

    def __init__(self, shared: list, client: list):
        self._id: Union[int, None] = None
        self.shared_keys = shared
        self.client_keys = client

    @property
    def id(self) -> Union[int, None]:
        """
        Get the unique ID for this attribute request.
        :return: Unique identifier for the request or None if not set.
        """
        return self._id

    @id.setter
    def id(self, value: int):
        """
        Set the unique ID for this attribute request.
        :param value: Unique identifier to set for the request.
        """
        if not isinstance(value, int):
            raise ValueError("ID must be an integer.")
        self._id = value

    def __repr__(self):
        return f"<AttributeRequest(id={self._id}, shared_keys={self.shared_keys}, client_keys={self.client_keys})>"

    def to_payload_format(self) -> dict:
        """
        Convert the attribute request to a payload format suitable for sending over MQTT to the platform.
        """
        formatted_request = {}
        if self.shared_keys is not None:
            formatted_request["sharedKeys"] = ','.join(self.shared_keys)
        if self.client_keys is not None:
            formatted_request["clientKeys"] = ','.join(self.client_keys)
        return formatted_request
