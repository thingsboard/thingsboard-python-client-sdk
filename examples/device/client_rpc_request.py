import psutil
import time
import logging
from tb_device_mqtt import TBClient
logging.basicConfig(level=logging.DEBUG)
client = TBClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")


def callback(request_id, resp_body):
    print(resp_body)


client.set_client_side_rpc_request_handler(callback)
client.connect()
client.send_rpc_call("getTime", {}, callback)
while True:
    pass
