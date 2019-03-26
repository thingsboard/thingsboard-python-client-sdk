import logging, logging.handlers
import time

import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)

ACCESS_TOKEN = "VSBk9a8nrkiGrMdDUEmm"
HOST = "demo.thingsboard.io"


def callback(result):
    print(result)


gw = tb.TBGateway(HOST, ACCESS_TOKEN)
gw.connect()

sub_id_2 = gw.subscribe_to_attributes("Test Device A2", callback)

while True:
    time.sleep(1.0)
    pass
