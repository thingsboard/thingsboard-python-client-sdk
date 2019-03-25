import logging
import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)
def callback(result):
    print(result)
client = tb.TBGateway("demo.thingsboard.io", "VSBk9a8nrkiGrMdDUEmm")
client.request_attributes("Example Name", ["temp"], type_is_client=False, callback=callback)
while True:
    pass