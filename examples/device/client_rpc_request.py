import psutil
import time
import logging
from tb_device_mqtt import TBClient
logging.basicConfig(level=logging.DEBUG)
client = TBClient("demo.thingsboard.io", "HvbKddqKsxVqowKoSR2J")


def callback(request_id, resp_body):
    print("request id: {request_id}, response body: {resp_body}".format(request_id=request_id,
                                                                        resp_body=resp_body))


client.set_client_side_rpc_request_handler(callback)
client.connect()
# call "getTime" on server and receive result, then process it with callback
client.send_rpc_call("getTime", {}, callback)
while True:
    time.sleep(1)
