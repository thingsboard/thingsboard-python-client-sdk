import logging
from tb_gateway_mqtt import TBGateway
logging.basicConfig(level=logging.DEBUG)
import time

gateway = TBGateway("demo.thingsboard.io", "VSBk9a8nrkiGrMdDUEmm")

gateway.connect()
gateway.send_telemetry(telemetry)
while True:
    pass
