import asyncio
import logging
import signal
import time
from datetime import UTC, datetime
from random import randint

from tb_mqtt_client.common.config_loader import DeviceConfig
from tb_mqtt_client.common.publish_result import PublishResult
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
MAX_PENDING_BATCHES = 100
FUTURE_TIMEOUT = 1.0


async def attribute_update_callback(update: AttributeUpdate):
    logger.info("Received attribute update: %r", update)


async def rpc_request_callback(request: RPCRequest):
    logger.info("Received RPC request: %r", request)
    return RPCResponse(request_id=request.request_id, result={"status": "ok"})


async def collect_futures_with_counts(future_or_result):
    """
    Accepts either a Future or a direct PublishResult.
    Returns [(Future or result, datapoint_count)] if successful, else [].
    """
    if future_or_result is None:
        return []

    if isinstance(future_or_result, PublishResult):
        if future_or_result.is_successful():
            return [(future_or_result, future_or_result.datapoints_count)]
        return []

    if isinstance(future_or_result, list):
        results = []
        for item in future_or_result:
            if isinstance(item, PublishResult):
                if item.is_successful():
                    results.append((item, item.datapoints_count))
            elif asyncio.isfuture(item) or asyncio.iscoroutine(item):
                results.extend(await collect_futures_with_counts(item))
            else:
                logger.warning("Unexpected item in list: %r", item)
        return results

    if asyncio.isfuture(future_or_result) or asyncio.iscoroutine(future_or_result):
        try:
            result = await asyncio.wait_for(asyncio.shield(future_or_result), timeout=FUTURE_TIMEOUT)
            if isinstance(result, PublishResult):
                return [(future_or_result, result.datapoints_count)]
            else:
                logger.warning("Unexpected publish result: %r", result)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for delivery future.")
        except Exception as e:
            logger.warning("Failed to get result from future: %s", e)
        return []

    logger.warning("collect_futures_with_counts() got unexpected type: %r", type(future_or_result))
    return []


async def process_pending_futures(pending_futures, delivered_batches, delivered_datapoints):
    try:
        all_futures = [fut for fut, _ in pending_futures]
        done, _ = await asyncio.wait(all_futures, timeout=FUTURE_TIMEOUT, return_when=asyncio.ALL_COMPLETED)
    except Exception as e:
        logger.warning("Wait exception: %s", e)
        return delivered_batches, delivered_datapoints

    for fut, count in pending_futures:
        if fut in done and fut.done() and not fut.cancelled():
            try:
                result = fut.result()
                if isinstance(result, PublishResult) and result.is_successful():
                    delivered_batches += 1
                    delivered_datapoints += result.datapoints_count
            except Exception as e:
                logger.warning("Future error: %s", e)
        elif not fut.done():
            fut.cancel()
            logger.warning("Cancelled future %r due to timeout.", getattr(fut, "uuid", None))

    return delivered_batches, delivered_datapoints


async def main():
    stop_event = asyncio.Event()

    def _shutdown_handler():
        stop_event.set()

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
    client._message_adapter.datapoints_max_count = 0
    logger.info("Connected to ThingsBoard.")

    sent_batches = 0
    delivered_batches = 0
    delivered_datapoints = 0
    pending_futures = []

    delivery_start_ts = None
    delivery_end_ts = None

    try:
        delivery_start_ts = time.perf_counter()
        ts_now = int(datetime.now(UTC).timestamp() * 1000)
        entries = [TimeseriesEntry(f"temperature{i}", randint(20, 40)) for i in range(BATCH_SIZE)]
        while not stop_event.is_set():

            try:
                fut = await client.send_timeseries(entries, wait_for_publish=False)
                future_pairs = await collect_futures_with_counts(fut)
                if future_pairs:
                    pending_futures.extend(future_pairs)
                else:
                    logger.warning("No child delivery futures detected (batch dropped?).")

                sent_batches += 1

            except Exception as e:
                logger.warning("Failed to publish telemetry batch: %s", e)

            if len(pending_futures) >= MAX_PENDING_BATCHES:
                delivered_batches, delivered_datapoints = await process_pending_futures(
                    pending_futures, delivered_batches, delivered_datapoints
                )
                pending_futures.clear()

            if sent_batches % 10 == 0:
                logger.info("Sent %d batches so far...", sent_batches)

            await asyncio.sleep(0)  # yield control efficiently

    finally:
        logger.info("Waiting for remaining telemetry batches to be acknowledged...")

        if pending_futures:
            delivered_batches, delivered_datapoints = await process_pending_futures(
                pending_futures, delivered_batches, delivered_datapoints
            )
        delivery_end_ts = time.perf_counter()

        await client.disconnect()
        logger.info("Disconnected cleanly.")

        if delivery_start_ts and delivery_end_ts:
            delivery_duration = delivery_end_ts - delivery_start_ts
            logger.info("Delivered %d batches / %d datapoints in %.6f seconds (%.0f datapoints/sec)",
                        delivered_batches, delivered_datapoints, delivery_duration,
                        delivered_datapoints / delivery_duration if delivery_duration > 0 else 0)
        else:
            logger.warning("No successful delivery occurred.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Interrupted by user.")
