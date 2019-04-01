import psutil
import time
import logging
from tb_device_mqtt import TBClient
logging.basicConfig(level=logging.DEBUG)
client = TBClient("127.0.0.1", "A2_TEST_TOKEN")
#client = TBClient("demo.thingsboard.io", "HvbKddqKsxVqowKoSR2J")


def callback(request_id, resp_body):
    print(resp_body)


client.set_client_side_rpc_request_handler(callback)
client.connect()
client.send_rpc_call("getTime", {}, callback)
while True:
    pass
