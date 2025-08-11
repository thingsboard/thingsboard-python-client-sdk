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
import math
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


@pytest.mark.asyncio
async def test_set_required_tokens_and_clear_event():
    rl = RateLimit("5:1", "token-test")
    rl.set_required_tokens(1, 3)
    # Initially event should not be set
    assert not rl.required_tokens_ready.is_set()
    # Force refill to satisfy requirement
    for bucket in rl._rate_buckets.values():
        bucket.tokens = 3
    await rl.refill()
    assert rl.required_tokens_ready.is_set()
    rl.clear_required_tokens_event()
    assert not rl.required_tokens_ready.is_set()


@pytest.mark.asyncio
async def test_refill_does_not_break_with_no_limit():
    rl = RateLimit("", "no-limit-refill")
    # Should not raise and should not change anything
    await rl.refill()
    assert rl._no_limit


@pytest.mark.asyncio
async def test_consume_with_real_limit_changes_tokens():
    rl = RateLimit("5:1", "consume-test")
    before = rl._rate_buckets[1].tokens
    await rl.consume(1)
    after = rl._rate_buckets[1].tokens
    assert after < before


def test_minimal_limit_and_timeout_no_limit_case():
    rl = RateLimit("", "min-test")
    assert rl.minimal_limit == 0
    assert rl.minimal_timeout == 0


@pytest.mark.asyncio
async def test_to_dict_contains_expected_structure():
    rl = RateLimit("5:1", "dict-test")
    d = rl.to_dict()
    assert "name" in d and d["name"] == "dict-test"
    assert "percentage" in d and isinstance(d["percentage"], int)
    assert "rateLimits" in d and isinstance(d["rateLimits"], dict)


@pytest.mark.asyncio
async def test_set_limit_with_no_entries_sets_no_limit():
    rl = RateLimit("5:1", "reset-test")
    await rl.set_limit("0:0,")
    assert not rl.has_limit()


@pytest.mark.asyncio
async def test_try_consume_returns_tuple_when_not_enough_tokens():
    rl = RateLimit("1:1", "tuple-test")
    # Exhaust tokens
    await rl.try_consume()
    res = await rl.try_consume()
    assert isinstance(res, tuple)
    assert len(res) == 2


def test_parse_string_with_invalid_entries():
    # This will hit logger.warning branch for parse_string exception
    rl = RateLimit("abc:def", "invalid-test")
    assert not rl.has_limit()


@pytest.mark.asyncio
async def test_reach_limit_when_no_limit_returns_none():
    rl = RateLimit("", "reach-none")
    assert await rl.reach_limit() is None


@pytest.mark.asyncio
async def test_reach_limit_resets_index_on_duration_window():
    rl = RateLimit("1:1,1:2", "reset-index")
    rl._RateLimit__reached_index = 10  # force overflow
    rl._RateLimit__reached_index_time = 0
    res = await rl.reach_limit()
    assert isinstance(res, tuple)
    assert rl._RateLimit__reached_index > 0


def test_env_variable_invalid(monkeypatch):
    # Simulate bad env variable
    monkeypatch.setenv("TB_DEFAULT_RATE_LIMIT_PERCENTAGE", "bad")
    # Reload module under test to re-run env reading
    import importlib
    import tb_mqtt_client.common.rate_limit.rate_limit as rl_mod
    importlib.reload(rl_mod)
    assert rl_mod.DEFAULT_RATE_LIMIT_PERCENTAGE == 80


def test_greedy_token_bucket_edge_cases():
    b = GreedyTokenBucket(2, 1)
    b.tokens = 0
    assert not b.can_consume(1)
    assert not b.consume(5)
    assert math.isclose(b.get_remaining_tokens(), b.tokens)


if __name__ == '__main__':
    pytest.main([__file__, "-v", "--tb=short"])
