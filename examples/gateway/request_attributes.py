import logging
import time

from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)


def callback(client, result, exception):
    client.stop()
    if exception is not None:
        print("Exception: " + str(exception))
    else:
        print(result)


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", "TEST_GATEWAY_TOKEN")
    gateway.connect()
    gateway.gw_request_shared_attributes("Example Name", ["temperature"], callback)

    while not gateway.stopped:
        time.sleep(1)


if __name__ == '__main__':
    main()
