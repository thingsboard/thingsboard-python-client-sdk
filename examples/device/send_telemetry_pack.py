import logging
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import time

logging.basicConfig(level=logging.DEBUG)

telemetry_with_ts = {"ts": int(round(time.time() * 1000)), "values": {"temperature": 42.1, "humidity": 70}}

client = TBDeviceMqttClient("127.0.0.1", "A2_TEST_TOKEN")
# we set maximum amount of messages sent to send them at the same time. it may stress memory but increases performance
client.max_inflight_messages_set(100)
client.connect()

results = []
result = True

for i in range(0, 100):
    results.append(client.send_telemetry(telemetry_with_ts))

for tmp_result in results:
    result &= tmp_result.get() == TBPublishInfo.TB_ERR_SUCCESS

print("Result " + str(result))

client.disconnect()
