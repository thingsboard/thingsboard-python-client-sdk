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
from time import sleep

import pytest

from tb_mqtt_client.common.rate_limit.rate_limit import RateLimit, GreedyTokenBucket


def test_greedy_token_bucket_can_consume_and_consume():
    bucket = GreedyTokenBucket(10, 1)
    assert bucket.can_consume(5) is True
    assert bucket.consume(5) is True
    assert bucket.tokens <= 5

    assert bucket.can_consume(6) is False
    assert bucket.consume(6) is False

    sleep(1.1)
    bucket.refill()
    assert round(bucket.tokens) == 10


def test_greedy_token_bucket_get_remaining_tokens_and_refill():
    bucket = GreedyTokenBucket(100, 2)
    bucket.consume(50)
    before = bucket.get_remaining_tokens()
    sleep(0.5)
    after = bucket.get_remaining_tokens()
    assert after > before


@pytest.mark.asyncio
async def test_rate_limit_no_limit_behavior():
    rl = RateLimit("", "test")
    assert await rl.check_limit_reached() is False
    assert await rl.try_consume() is None
    assert await rl.consume() is None
    assert rl.minimal_limit == 0
    assert rl.minimal_timeout == 0
    assert rl.has_limit() is False
    assert await rl.reach_limit() is None
    assert rl.to_dict()["no_limit"] is True


@pytest.mark.asyncio
async def test_rate_limit_basic_limit_behavior():
    rl = RateLimit("10:1,20:2", "test")
    assert rl.has_limit() is True
    assert rl.minimal_limit == 10 * 0.8
    assert rl.minimal_timeout >= 2

    for _ in range(8):
        assert await rl.try_consume() is None

    warning = await rl.try_consume()
    assert isinstance(warning, tuple)
    assert warning[0] == 8.0
    assert warning[1] == 1.0

    exceeded = await rl.try_consume()
    assert isinstance(exceeded, tuple)

    await rl.consume()


@pytest.mark.asyncio
async def test_rate_limit_reach_limit_rotation():
    rl = RateLimit("1:1,1:2", "test")
    await rl.reach_limit()
    await rl.reach_limit()
    result = await rl.reach_limit()

    assert isinstance(result, tuple)
    index, timestamp, dur = result
    assert index >= 0
    assert dur in (1, 2)


@pytest.mark.asyncio
async def test_rate_limit_set_limit_resets_state():
    rl = RateLimit("100:10", "test")
    original = rl.to_dict()
    await rl.set_limit("10:1", percentage=50)
    new_state = rl.to_dict()

    assert new_state["rateLimits"] != original["rateLimits"]
    assert rl.percentage == 50


@pytest.mark.asyncio
async def test_rate_limit_invalid_format(caplog):
    rl = RateLimit("invalid_format,10:xyz", "bad_config")
    assert rl.has_limit() is False or isinstance(rl.to_dict(), dict)
    assert any("Invalid rate limit format" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_rate_limit_refill_behavior():
    rl = RateLimit("3:1", "refill-test")
    await rl.try_consume()
    await rl.try_consume()
    await rl.try_consume()
    assert (await rl.try_consume()) is not None

    await asyncio.sleep(1.1)
    await rl.refill()
    assert (await rl.try_consume()) is None
