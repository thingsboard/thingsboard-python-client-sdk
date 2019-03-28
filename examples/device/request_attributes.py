import logging
import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = tb.TBClient("127.0.0.1", "A1_TEST_TOKEN")
client.connect()
client.request_attributes(["temp"], callback=callback)
while True:
    pass
