import logging.handlers
import time

import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print("Callback for attributes, {0}".format(result))


def callback_for_everything(result):
    print("Everything goes here, {0}".format(result))


def callback_for_specific_attr(result):
    print("Specific attribute callback, {0}".format(result))


gw = tb.TBGateway("demo.thingsboard.io", "HvbKddqKsxVqowKoSR2J")
gw.connect()
gw.connect_device("Test Device A2")

gw.subscribe_to_all(callback_for_everything)

gw.subscribe_to_attribute("Test Device A2", "temperature", callback_for_specific_attr)

sub_id = gw.subscribe_to_attributes("Test Device A2", callback)
gw.unsubscribe(sub_id)

while True:
    time.sleep(1)
