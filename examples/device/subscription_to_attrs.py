import logging
import time

import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = tb.TBClient("127.0.0.1", "A2_TEST_TOKEN")
client.connect()
sub_id_1 = client.subscribe("temperature", callback)
sub_id_2 = client.subscribe_to_everything(callback)
client.unsubscribe(sub_id_1)
while True:
    time.sleep(1)
