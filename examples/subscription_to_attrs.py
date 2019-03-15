import logging, logging.handlers
import tb_mqtt_client as tb
logging.basicConfig(level=logging.INFO)


def callback(result):
    print(result)


client = tb.TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
client.connect()
sub_id = client.subscribe(callback, "ololo")
client.unsubscribe(sub_id)
