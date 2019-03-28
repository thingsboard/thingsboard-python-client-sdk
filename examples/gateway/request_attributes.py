import logging
import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = tb.TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
client.request_shared_attributes("Example Name", ["temp"], callback)
while True:
    pass