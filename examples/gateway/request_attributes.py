import logging
import time

import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


gw = tb.TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
gw.connect()
gw.request_shared_attributes("Example Name", ["temperature"], callback)
while True:
    time.sleep(1)
