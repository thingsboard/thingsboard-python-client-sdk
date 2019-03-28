import logging
from tb_gateway_mqtt import TBGateway
logging.basicConfig(level=logging.DEBUG)

gateway = TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
gateway.connect_device("Example Name")
#disconnect wont delete device from TB server, it just stops sending updates for this one to gateway
gateway.disconnect_device("Example Name")
