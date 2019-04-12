import logging
import time

from tb_device_mqtt import TBDeviceMqttClient
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = TBDeviceMqttClient("127.0.0.1", "A2_TEST_TOKEN")
client.connect()
sub_id_1 = client.subscribe_to_attribute("uploadFrequency", callback)
sub_id_2 = client.subscribe_to_all_attributes(callback)
client.unsubscribe_from_attribute(sub_id_1)
client.unsubscribe_from_attribute(sub_id_2)
while True:
    time.sleep(1)
