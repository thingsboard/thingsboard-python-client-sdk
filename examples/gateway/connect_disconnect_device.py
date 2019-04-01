import logging
from tb_gateway_mqtt import TBGateway
logging.basicConfig(level=logging.DEBUG)

gateway = TBGateway("demo.thingsboard.io", "HvbKddqKsxVqowKoSR2J")
gateway.connect()
gateway.connect_device("Example Name")
gateway.disconnect_device("Example Name")
gateway.disconnect()
