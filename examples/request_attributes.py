import logging
import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = tb.TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
client.connect()
client.request_attributes(["temp"], callback=callback)
while True:
    pass
