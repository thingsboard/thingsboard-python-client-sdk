import logging
from tb_gateway_mqtt import TBGateway
logging.basicConfig(level=logging.DEBUG)

gateway = TBGateway("demo.thingsboard.io", "VSBk9a8nrkiGrMdDUEmm")
gateway.connect_device("Example Name")
#disconnect wont delete device from TB server, it just stops sending updates for this one to gateway
gateway.disconnect_device("Example Name")


