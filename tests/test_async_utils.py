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
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from tb_mqtt_client.common.async_utils import await_and_resolve_original


class TestAwaitAndResolveOriginal(IsolatedAsyncioTestCase):

    async def resolves_parent_with_first_successful_child_result(self):
        parent_future = asyncio.Future()
        child_future_1 = asyncio.Future()
        child_future_2 = asyncio.Future()

        child_future_1.set_result("success")
        child_future_2.set_exception(ValueError("error"))

        await await_and_resolve_original([parent_future], [child_future_1, child_future_2])

        self.assertTrue(parent_future.done())
        self.assertEqual(parent_future.result(), "success")

    async def resolves_parent_with_first_exception_if_no_successful_results(self):
        parent_future = asyncio.Future()
        child_future_1 = asyncio.Future()
        child_future_2 = asyncio.Future()

        child_future_1.set_exception(ValueError("error1"))
        child_future_2.set_exception(ValueError("error2"))

        await await_and_resolve_original([parent_future], [child_future_1, child_future_2])

        self.assertTrue(parent_future.done())
        self.assertIsInstance(parent_future.exception(), ValueError)
        self.assertEqual(str(parent_future.exception()), "error1")

    async def handles_empty_child_futures_list(self):
        parent_future = asyncio.Future()

        await await_and_resolve_original([parent_future], [])

        self.assertTrue(parent_future.done())
        self.assertIsNone(parent_future.result())

    async def sets_exception_on_parent_if_unexpected_error_occurs(self):
        parent_future = asyncio.Future()
        child_future = asyncio.Future()

        with patch("tb_mqtt_client.common.async_utils.future_map.child_resolved", side_effect=Exception("unexpected")):
            await await_and_resolve_original([parent_future], [child_future])

        self.assertTrue(parent_future.done())
        self.assertIsInstance(parent_future.exception(), Exception)
        self.assertEqual(str(parent_future.exception()), "unexpected")