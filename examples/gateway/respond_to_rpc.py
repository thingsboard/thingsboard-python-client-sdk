import logging.handlers
import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


gw = tb.TBGateway("demo.thingsboard.io", "VSBk9a8nrkiGrMdDUEmm")
gw.connect()

gw.connect_device("Test Device A1")

gw.start_getting_rpc()
while True:
    pass
