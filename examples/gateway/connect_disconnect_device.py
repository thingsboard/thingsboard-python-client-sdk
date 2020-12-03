import logging
from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", "TEST_GATEWAY_TOKEN")
    gateway.connect()
    gateway.gw_connect_device("Example Name")
    # device disconnecting will not delete device, gateway just stops receiving messages
    gateway.gw_disconnect_device("Example Name")
    gateway.disconnect()


if __name__ == '__main__':
    main()
