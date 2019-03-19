import logging
import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)

ACCESS_TOKEN = "v5cgxxXGHvuFwdxENEc7"
HOST = "demo.thingsboard.io"

def callback(result):
    print(result)

client = tb.TbClient(HOST, ACCESS_TOKEN)
client.connect()
client.request_attributes(["temp"], callback=callback)
while True: pass
