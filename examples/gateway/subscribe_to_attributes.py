import logging.handlers
import time

from tb_gateway_mqtt import TBGatewayMqttClient
logging.basicConfig(level=logging.DEBUG)


def callback(gateway, result):
    logging.debug("Gateway %r. Callback for attributes, %r", gateway, result)


def callback_for_everything(gateway, result):
    logging.debug("Gateway %r. Everything goes here, %r", gateway, result)


def callback_for_specific_attr(gateway, result):
    logging.debug("Gateway %r. Specific attribute callback, %r", gateway, result)


def main():
    gateway = TBGatewayMqttClient("127.0.0.1", "TEST_GATEWAY_TOKEN")
    gateway.connect()
    # without device connection it is impossible to get any messages
    gateway.gw_connect_device("Test Device A2")

    gateway.gw_subscribe_to_all_attributes(callback_for_everything)

    gateway.gw_subscribe_to_attribute("Test Device A2", "temperature", callback_for_specific_attr)

    sub_id = gateway.gw_subscribe_to_all_device_attributes("Test Device A2", callback)
    gateway.gw_unsubscribe(sub_id)

    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
