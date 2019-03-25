import logging, logging.handlers
import tb_device_mqtt as tb
logging.basicConfig(level=logging.DEBUG)

ACCESS_TOKEN = "HvbKddqKsxVqowKoSR2J"
HOST = "demo.thingsboard.io"

def callback_for_everythings(result):
    print("lol kek")
    print(result)

def callback(result):
    print("dat callback")
    print(result)


client = tb.TBClient(HOST, ACCESS_TOKEN)
client.connect()
#sub_id = client.subscribe("*", callback_for_everythings)
sub_id = client.subscribe("lol", callback)

client.unsubscribe(sub_id)
while True:
    pass
#client.unsubscribe(sub_id)
