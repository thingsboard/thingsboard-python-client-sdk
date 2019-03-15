import logging, logging.handlers
import tb_mqtt_client as tb
logging.basicConfig(level=logging.INFO)

ACCESS_TOKEN = "v5cgxxXGHvuFwdxENEc7"
HOST = "demo.thingsboard.io"

def callback(result):
    print(result)


client = tb.TbClient(HOST, ACCESS_TOKEN)
client.connect()
sub_id = client.subscribe(callback, "ololo")
client.unsubscribe(sub_id)
