import logging
import time

from tb_device_mqtt import TBDeviceMqttClient
logging.basicConfig(level=logging.DEBUG)


def on_attributes_change(client, result, exception):
    client.stop()
    if exception is not None:
        print("Exception: " + str(exception))
    else:
        print(result)


def main():
    client = TBDeviceMqttClient("127.0.0.1", "A2_TEST_TOKEN")
    client.connect()
    client.request_attributes(["atr1", "atr2"], callback=on_attributes_change)
    while not client.stopped:
        time.sleep(1)


if __name__ == '__main__':
    main()
