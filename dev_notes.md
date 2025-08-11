# Developer Notes for ThingsBoard Python Client SDK

## Overview
This document provides explanations of tricky moments and major points in the ThingsBoard Python Client SDK implementation. It focuses on key areas that require special attention for developers working with or maintaining the codebase.

## GMQTT Patches

The SDK uses the GMQTT library for MQTT communication but implements several patches to enhance its functionality:

### Key Components:
- `tb_mqtt_client/common/gmqtt_patch.py`: Contains the `PatchUtils` class that applies various patches to the GMQTT library.

### Major Patches:

1. **Enhanced Disconnect Handling**:
   - The original GMQTT disconnect handling is patched to provide better error reporting and recovery.
   - `patch_mqtt_handler_disconnect()` modifies how disconnect packets are processed, adding proper reason code extraction.

2. **Connection Acknowledgment Handling**:
   - `patch_handle_connack()` enhances the connection acknowledgment process to better handle MQTT 5.0 properties.

3. **Connection Lost Recovery**:
   - `patch_gmqtt_protocol_connection_lost()` improves how the client handles unexpected disconnections.
   - This is critical for maintaining robust connections in unstable network environments.

4. **PUBACK Handling with Reason Codes**:
   - `patch_puback_handling()` adds support for MQTT 5.0 reason codes in publish acknowledgments.
   - This allows the client to understand why a message was rejected by the broker.

5. **Message Storage for Retries**:
   - `patch_storage()` modifies how messages are stored to support the retry mechanism.

## Message Queues

The SDK implements a sophisticated message queuing system to handle message processing, rate limiting, and retries:

### Key Components:
- `tb_mqtt_client/common/queue.py`: Implements `AsyncDeque`, a thread-safe asynchronous queue.
- `tb_mqtt_client/service/message_service.py`: Contains the `MessageService` class that manages multiple queues.

### Queue Types:
1. **Initial Queue**: 
   - Entry point for all messages before they're routed to specialized queues.
   - Implemented in `MessageService._dispatch_initial_queue_loop()`.

2. **Service Queue**: 
   - Handles general service messages.

3. **Device Uplink Messages Queue**: 
   - Specifically for device telemetry and attribute updates.

4. **Gateway Uplink Messages Queue**: 
   - For gateway-specific messages that may contain data for multiple devices.

5. **Retry by QoS Queue**: 
   - Special queue for handling QoS 1 messages that need to be retried.
   - Implemented in `MessageService._dispatch_retry_by_qos_queue_loop()`.

### Queue Processing:
- Each queue has its own asynchronous processing loop.
- Messages are processed according to rate limits and QoS requirements.
- The system uses `asyncio` for non-blocking queue operations.

## QoS Messages Reprocessing

The SDK implements a robust mechanism for handling QoS 1 messages, ensuring they are delivered at least once:

### Key Components:
- `tb_mqtt_client/service/mqtt_manager.py`: Contains the `patch_client_for_retry_logic()` method.
- `tb_mqtt_client/common/gmqtt_patch.py`: Implements the retry mechanism in `_retry_loop()`.

### Retry Process:
1. **Message Tracking**:
   - Messages with QoS 1 are tracked until acknowledgment is received.
   - If no acknowledgment is received within the timeout, the message is requeued.

2. **Retry Loop**:
   - `PatchUtils._retry_loop()` periodically checks for unacknowledged messages.
   - Messages that haven't been acknowledged are put back into the retry queue.

3. **Integration with Message Service**:
   - `MQTTManager.patch_client_for_retry_logic()` connects the GMQTT client with the message service's retry queue.
   - `MessageService.put_retry_message()` handles requeuing of messages that need to be retried.

4. **Publish Monitoring**:
   - `MQTTManager._monitor_ack_timeouts()` and `check_pending_publishes()` track publish operations and handle timeouts.

## Gateway Message Dispatcher

The gateway part of the SDK includes a message dispatcher for handling various types of events:

### Key Components:
- `tb_mqtt_client/service/gateway/direct_event_dispatcher.py`: Implements the `DirectEventDispatcher` class.

### Dispatcher Features:
1. **Event Registration**:
   - Handlers can be registered for specific event types using `register()`.
   - Multiple handlers can be registered for the same event type.

2. **Event Dispatching**:
   - `dispatch()` method routes events to the appropriate handlers.
   - Supports both synchronous and asynchronous handlers.

3. **Device-Specific Handling**:
   - Can route events directly to a specific device session if provided.
   - Otherwise, broadcasts to all registered handlers for the event type.

4. **Error Handling**:
   - Catches and logs exceptions in handlers to prevent one handler failure from affecting others.

## Message Splitting

The SDK includes mechanisms to handle large messages by splitting them into smaller chunks:

### Key Components:
- `tb_mqtt_client/service/base_message_splitter.py`: Defines the base interface.
- `tb_mqtt_client/service/gateway/message_splitter.py`: Implements gateway-specific splitting logic.

### Splitting Strategy:
1. **Size-Based Splitting**:
   - Messages are split if they exceed the maximum payload size (default 55KB).
   - Each split message maintains the original message's context.

2. **Datapoint-Based Splitting**:
   - Messages can also be split based on the number of datapoints.
   - This helps prevent overwhelming the server with too many datapoints in a single message.

3. **Message grouping**:
   - Uplink data messages may be grouped to efficiently use the available payload size.
   - This reduces the number of messages sent and optimizes network usage.

4. **Future Chaining**:
   - When a message is split, the futures from the original message are chained to the split messages.
   - This ensures that the completion status is properly propagated back to the caller.

## Rate Limiting

The SDK implements rate limiting to prevent overwhelming the ThingsBoard server:

### Key Components:
- `tb_mqtt_client/common/rate_limit/rate_limit.py`: Defines rate limit structures.
- `tb_mqtt_client/common/rate_limit/rate_limiter.py`: Implements the rate limiting logic.

### Rate Limiting Features:
1. **Server-Provided Limits**:
   - The SDK requests rate limits from the server upon connection.
   - These limits are then applied to message publishing.

2. **Separate Device and Gateway Limits**:
   - Different rate limits can be applied to device and gateway messages.

3. **Backpressure Control**:
   - When rate limits are exceeded, the system applies backpressure to slow down message production.



## Futures in Device and Gateway Uplink Messages

### Overview of Futures in the SDK

Futures are a critical component in the ThingsBoard Python Client SDK for handling asynchronous operations, particularly for message publishing. They provide a way to track the status of message delivery and propagate results back to the caller.

### Futures in Uplink Messages

#### Device and Gateway Message Structure

Both `DeviceUplinkMessage` and `GatewayUplinkMessage` classes contain a `delivery_futures` field, which is a list of `asyncio.Future` objects. These futures are resolved when the message is successfully published to the ThingsBoard server or when an error occurs.

```python
# From DeviceUplinkMessage
delivery_futures: List[Optional[asyncio.Future[PublishResult]]]
```

When a client application sends data to ThingsBoard, it can attach a future to the message to be notified when the message is delivered:

```python
# Example usage
future = asyncio.get_running_loop().create_future()
message_builder.add_delivery_futures(future)
message = message_builder.build()
# Later, the application can await future to know when the message is delivered
result = await future  # PublishResult object
```

### Message Splitting and Future Chaining

One of the trickiest aspects of the SDK is how it handles futures when messages need to be split due to size limitations or datapoint count restrictions.

#### The Splitting Process

When a message exceeds the maximum payload size (default 55KB) or the maximum number of datapoints, it needs to be split into smaller chunks. This presents a challenge: how to maintain the relationship between the original message's future and the futures of the split messages?

#### Future Chaining Mechanism

The SDK uses a sophisticated future chaining mechanism implemented in the `FutureMap` class:

```python
# From async_utils.py
class FutureMap:
    def __init__(self):
        self._child_to_parents: Dict[asyncio.Future, Set[asyncio.Future]] = {}
        self._parent_to_remaining: Dict[asyncio.Future, Set[asyncio.Future]] = {}
```

This class maintains two mappings:
1. `_child_to_parents`: Maps each child future (from split messages) to its parent futures
2. `_parent_to_remaining`: Maps each parent future to the set of child futures that still need to be resolved

##### Registration Process

When a message is split, new futures are created for each split message, and these are registered with the original message's future:

```python
# In message_splitter.py
shared_future = asyncio.get_running_loop().create_future()
shared_future.uuid = uuid4()
builder.add_delivery_futures(shared_future)

built = builder.build()
result.append(built)
for parent in parent_futures:
    future_map.register(parent, [shared_future])
```

##### Resolution Process

When a child future is resolved (when a split message is delivered), the `child_resolved` method is called:

```python
# In FutureMap.child_resolved
def child_resolved(self, child: asyncio.Future):
    parents = self._child_to_parents.pop(child, set())
    for parent in parents:
        remaining = self._parent_to_remaining.get(parent)
        if remaining is not None:
            remaining.discard(child)
            if not remaining and not parent.done():
                # All children resolved, resolve the parent
                all_children = list(remaining) + [child]
                results = []
                for f in all_children:
                    if f.done() and not f.cancelled():
                        result = f.result()
                        if isinstance(result, PublishResult):
                            results.append(result)

                if results:
                    parent.set_result(PublishResult.merge(results))
                else:
                    parent.set_result(None)
```

This ensures that the parent future is only resolved when all of its child futures are resolved, and the results are merged.

#### Differences Between Device and Gateway Splitters

While the core mechanism is the same, there are some differences in how device and gateway message splitters handle futures:

1. **Device Message Splitter**:
   - Focuses on splitting individual device messages
   - Handles timeseries and attributes separately
   - Creates a shared future for each batch of data

2. **Gateway Message Splitter**:
   - Handles messages from multiple devices
   - Groups data by device name and profile
   - Maintains the device context across split messages

### MQTT Manager's Role

The `MQTTManager` class plays a crucial role in the future resolution process:

```python
# In mqtt_manager.py
@staticmethod
async def _add_future_chain_processing(mqtt_future, message: MqttPublishMessage):
    def resolve_attached(publish_future: asyncio.Future):
        try:
            try:
                publish_result = publish_future.result()
            except asyncio.CancelledError:
                publish_result = PublishResult(message.topic, message.qos, -1, len(message.payload), -1)
            except Exception as exc:
                publish_result = PublishResult(message.topic, message.qos, -1, len(message.payload), -1)

            for i, f in enumerate(message.delivery_futures or []):
                if f is not None and not f.done():
                    f.set_result(publish_result)
                    future_map.child_resolved(f)
        except Exception as e:
            for i, f in enumerate(message.delivery_futures or []):
                if f is not None and not f.done():
                    f.set_exception(e)
```

This method:
1. Attaches a callback to the MQTT publish future
2. When the publish operation completes, it resolves all the delivery futures attached to the message
3. Calls `future_map.child_resolved()` to propagate the resolution up the chain

### Practical Implications

This future chaining mechanism has several important implications:

1. **Transparent Splitting**: Applications don't need to be aware that their messages are being split. They attach a future to the original message and receive a notification when all parts are delivered.

2. **Error Handling**: If any part of a split message fails to deliver, the error is propagated to the parent future.

3. **Performance Optimization**: The SDK can optimize message delivery by splitting large messages without breaking the promise to notify the application when delivery is complete.

4. **Resource Management**: Futures are properly managed and resolved, preventing memory leaks even when messages are split into many parts.

## Conclusion

The ThingsBoard Python Client SDK implements several sophisticated mechanisms to ensure reliable message delivery, efficient processing, and robust error handling.  
The future mechanism in the ThingsBoard Python Client SDK provides a robust way to handle asynchronous message delivery with transparent message splitting.  
Understanding this mechanism is crucial for developers working with the SDK, especially when dealing with large messages or high-throughput scenarios.  

Key areas to be aware of when making changes:
1. The GMQTT patches that enhance the underlying MQTT library
2. The message queue system that manages message flow
3. The QoS message reprocessing mechanism that ensures reliable delivery
4. The gateway message dispatcher that routes events to appropriate handlers
5. The message splitting logic that handles large payloads
6. The rate limiting system that prevents overwhelming the server