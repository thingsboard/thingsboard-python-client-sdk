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
from datetime import datetime, UTC
from random import randint

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.entities.data.timeseries_entry import TimeseriesEntry
from tb_mqtt_client.entities.data.attribute_update import AttributeUpdate
from tb_mqtt_client.entities.data.rpc_request import RPCRequest
from tb_mqtt_client.entities.data.rpc_response import RPCResponse
from tb_mqtt_client.service.device.client import DeviceClient
from tb_mqtt_client.common.logging_utils import configure_logging, get_logger

# --- Logging setup ---
configure_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("tb_mqtt_client").setLevel(logging.INFO)

# --- Constants ---
BATCH_SIZE = 1000
YIELD_DELAY = 0.001
MAX_PENDING_BATCHES = 100
FUTURE_TIMEOUT = 1.0


async def attribute_update_callback(update: AttributeUpdate):
    logger.info("Received attribute update: %r", update)


async def rpc_request_callback(request: RPCRequest):
    logger.info("Received RPC request: %r", request)
    return RPCResponse(request_id=request.request_id, result={"status": "ok"})


async def main():
    stop_event = asyncio.Event()

    def _shutdown_handler():
        stop_event.set()
        asyncio.get_event_loop().run_until_complete(client.stop())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _shutdown_handler())

    config = DeviceConfig()
    config.host = "localhost"
    config.access_token = "YOUR_ACCESS_TOKEN"

    client = DeviceClient(config)
    client.set_attribute_update_callback(attribute_update_callback)
    client.set_rpc_request_callback(rpc_request_callback)

    await client.connect()
    logger.info("Connected to ThingsBoard.")

    sent_batches = 0
    delivered_batches = 0
    delivered_datapoints = 0
    pending_futures = []

    delivery_start_ts = None  # Start time of the first successful delivery
    delivery_end_ts = None    # End time of last successful delivery

    try:
        while not stop_event.is_set():
            ts_now = int(datetime.now(UTC).timestamp() * 1000)
            entries = [
                TimeseriesEntry("temperature", randint(20, 40), ts=ts_now - i)
                for i in range(BATCH_SIZE)
            ]

            try:
                future = await client.send_telemetry(entries)
                if future:
                    pending_futures.append((future, BATCH_SIZE))
                    sent_batches += 1
                else:
                    logger.warning("Telemetry batch dropped or not acknowledged.")
            except Exception as e:
                logger.warning("Failed to publish telemetry batch: %s", e)

            if len(pending_futures) >= MAX_PENDING_BATCHES:
                done, _ = await asyncio.wait(
                    [f for f, _ in pending_futures], timeout=FUTURE_TIMEOUT
                )

                remaining = []
                for fut, batch_size in pending_futures:
                    if fut in done:
                        try:
                            result = fut.result()
                            if result is True:
                                delivered_batches += 1
                                delivered_datapoints += batch_size
                                now = time.perf_counter()
                                delivery_start_ts = delivery_start_ts or now
                                delivery_end_ts = now
                        except asyncio.CancelledError:
                            logger.exception("Future was cancelled: %r, id: %r", fut, id(fut))
                            logger.warning("Delivery future was cancelled: %r", fut)
                        except Exception as e:
                            logger.warning("Delivery future raised: %s", e)
                    else:
                        fut.cancel()
                        logger.warning("Cancelled delivery future after timeout: %r, future id: %r", fut, id(fut))
                        # remaining.append((fut, batch_size))

                pending_futures = []

            if sent_batches % 10 == 0:
                logger.info("Sent %d batches so far...", sent_batches)

            await asyncio.sleep(YIELD_DELAY)

    finally:
        logger.info("Waiting for remaining telemetry batches to be acknowledged...")
        done, _ = await asyncio.wait(
            [f for f, _ in pending_futures], timeout=.01
        )

        for fut, batch_size in pending_futures:
            if fut in done:
                try:
                    result = fut.result()
                    if result is True:
                        delivered_batches += 1
                        delivered_datapoints += batch_size
                        now = time.perf_counter()
                        delivery_start_ts = delivery_start_ts or now
                        delivery_end_ts = now
                except asyncio.CancelledError:
                    # logger.warning("Final delivery future was cancelled: %r", fut)
                    pass
                except Exception as e:
                    logger.warning("Final delivery failed: %s", e)
            else:
                fut.cancel()
                # logger.warning("Final delivery future timed out and was cancelled: %r", fut)

        await client.disconnect()
        logger.info("Disconnected cleanly.")

        if delivery_start_ts is not None and delivery_end_ts is not None:
            delivery_duration = delivery_end_ts - delivery_start_ts
            logger.info("Delivered %d batches / %d datapoints in %.6f seconds (%.0f datapoints/sec)",
                        delivered_batches, delivered_datapoints, delivery_duration,
                        delivered_datapoints / delivery_duration if delivery_duration > 0 else 0)
        else:
            logger.warning("No successful delivery occurred.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user.")
