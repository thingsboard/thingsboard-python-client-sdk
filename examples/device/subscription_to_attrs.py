import logging, logging.handlers
import tb_device_mqtt as tb
logging.basicConfig(level=logging.INFO)

ACCESS_TOKEN = "v5cgxxXGHvuFwdxENEc7"
HOST = "demo.thingsboard.io"

def callback(result):
    print(result)


client = tb.TBClient(HOST, ACCESS_TOKEN)
client.connect()
sub_id = client.subscribe("ololo", callback)

client.unsubscribe(sub_id)
