import logging.handlers
import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


gw = tb.TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
gw.connect()

gw.connect_device("Test Device A1")

gw.start_getting_rpc()
while True:
    pass
