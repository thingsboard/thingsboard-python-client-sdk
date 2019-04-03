import logging
import time

import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result, exception):
    if exception is not None:
        print("Exception: " + str(exception))
    else:
        print(result)


client = tb.TBClient("127.0.0.1", "A2_TEST_TOKEN")
client.connect()
client.request_attributes(["atr1", "atr2"], callback=callback)
while True:
    time.sleep(1)
