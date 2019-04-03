import logging
import time

import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result, exception):
    if exception is not None:
        print("Exception: " + str(exception))
        print("Exception: " + exception.message)
    else:
        print(result)


client = tb.TBClient("127.0.0.1", "A1_TEST_TOKEN")
client.connect()
client.request_attributes(["atr1"], callback=callback)
client.request_attributes(["atr2"], callback=callback)
while True:
    time.sleep(1)
