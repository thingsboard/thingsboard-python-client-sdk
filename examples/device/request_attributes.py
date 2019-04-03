import logging
import time

import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = tb.TBClient("demo.thingsboard.io", "HvbKddqKsxVqowKoSR2J")
client.connect()
client.request_attributes(["atr1"], callback=callback)
while True:
    time.sleep(1)
