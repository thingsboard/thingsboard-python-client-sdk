import logging
from tb_gateway_mqtt import TBGateway
logging.basicConfig(level=logging.DEBUG)

gateway = TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
gateway.connect()
gateway.connect_device("Example Name")
# device disconnecting will not delete device, gateway just stops receiving messages
gateway.disconnect_device("Example Name")
gateway.disconnect()

