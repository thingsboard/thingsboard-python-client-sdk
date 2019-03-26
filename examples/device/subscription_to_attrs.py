import logging, logging.handlers
import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)

ACCESS_TOKEN = "HvbKddqKsxVqowKoSR2J"
HOST = "demo.thingsboard.io"


def callback(result):
    print(result)


client = tb.TBClient(HOST, ACCESS_TOKEN)
client.connect()
sub_id_1 = client.subscribe(callback, "temperature")
sub_id_2 = client.subscribe(callback, "*")
client.unsubscribe(sub_id_1)
while True:
    pass
