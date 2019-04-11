import logging
import time

from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)


def callback(result, exception):
    if exception is not None:
        print("Exception: " + str(exception))
    else:
        print(result)


gateway = TBGatewayMqttClient("127.0.0.1", "TEST_GATEWAY_TOKEN")
gateway.connect()
gateway.gw_request_shared_attributes("Example Name", ["temperature"], callback)

while True:
    time.sleep(1)
