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
import logging
import signal
import time
from datetime import UTC, datetime
from random import randint
from typing import List

from tb_mqtt_client.common.config_loader import GatewayConfig
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.service.gateway.client import GatewayClient
from tb_mqtt_client.service.gateway.device_session import DeviceSession
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger
from tb_mqtt_client.common.publish_result import PublishResult

# --- Logging ---
configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)

# --- Constants ---
NUM_DEVICES = 2
BATCH_SIZE = 100
MAX_PENDING = 100
FUTURE_TIMEOUT = 1.0
DEVICE_PREFIX = "perf-test-device"
WAIT_FOR_PUBLISH = False

# --- Test logic ---
async def send_batch(client: GatewayClient, session: DeviceSession) -> List[asyncio.Future]:
    ts = int(datetime.now(UTC).timestamp() * 1000)
    entries = [TimeseriesEntry(f"temp{i}", randint(20, 35), ts=ts) for i in range(BATCH_SIZE)]
    result = await client.send_device_timeseries(session, entries, wait_for_publish=WAIT_FOR_PUBLISH)
    if isinstance(result, list):
        return result
    elif result is not None:
        return [result]
    return []

async def wait_for_futures(futures: List[asyncio.Future]) -> int:
    delivered = 0
    done, _ = await asyncio.wait(futures, timeout=FUTURE_TIMEOUT, return_when=asyncio.ALL_COMPLETED)
    for fut in done:
        try:
            res = fut.result()
            if isinstance(res, PublishResult) and res.is_successful():
                delivered += res.datapoints_count
        except Exception as e:
            logger.warning("Future error: %s", e)
    return delivered


async def main():
    stop_event = asyncio.Event()

    def _shutdown():
        logger.info("Shutting down by signal...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _shutdown())

    config = GatewayConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = GatewayClient(config)
    await client.connect()

    logger.info("Connected to ThingsBoard as gateway")

    # Register test devices
    sessions: List[DeviceSession] = []
    for i in range(NUM_DEVICES):
        device_name = f"{DEVICE_PREFIX}-{i}"
        session, _ = await client.connect_device(device_name, wait_for_publish=False)
        sessions.append(session)

    logger.info("Registered %d devices", len(sessions))

    sent_batches = 0
    delivered_dp = 0
    pending_futures: List[asyncio.Future] = []

    start = time.perf_counter()

    try:
        while not stop_event.is_set():
            for session in sessions:
                futs = await send_batch(client, session)
                pending_futures.extend(futs)
                sent_batches += 1

            if len(pending_futures) >= MAX_PENDING:
                delivered = await wait_for_futures(pending_futures)
                delivered_dp += delivered
                pending_futures.clear()
                logger.info("Delivered datapoints so far: %d", delivered_dp)

            await asyncio.sleep(0)  # yield to event loop

    finally:
        logger.info("Flushing remaining futures...")
        if pending_futures:
            delivered_dp += await wait_for_futures(pending_futures)
        end = time.perf_counter()

        duration = end - start
        logger.info("Sent %d batches across %d devices", sent_batches, NUM_DEVICES)
        logger.info("Delivered %d datapoints in %.2f seconds (%.0f datapoints/sec)",
                    delivered_dp, duration, delivered_dp / duration if duration > 0 else 0)
        await client.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Interrupted by user.")
