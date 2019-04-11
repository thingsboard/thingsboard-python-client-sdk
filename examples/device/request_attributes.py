import logging
import time

from tb_device_mqtt import TBDeviceMqttClient
logging.basicConfig(level=logging.DEBUG)


def on_attributes_change(result, exception):
    if exception is not None:
        print("Exception: " + str(exception))
    else:
        print(result)


client = TBDeviceMqttClient("127.0.0.1", "A2_TEST_TOKEN")
client.connect()
client.request_attributes(["atr1", "atr2"], callback=on_attributes_change)
while True:
    time.sleep(1)
