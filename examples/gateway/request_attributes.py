import logging
import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = tb.TBGateway("demo.thingsboard.io", "VSBk9a8nrkiGrMdDUEmm")

client.connect()
client.request_shared_attributes("Example Name", ["temp"], callback)
while True:
    pass
