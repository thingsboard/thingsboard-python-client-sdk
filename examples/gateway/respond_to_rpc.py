import logging.handlers
import time

import tb_gateway_mqtt as tb
import psutil
logging.basicConfig(level=logging.DEBUG)


def rpc_request_response(request_body):
    # request body contains id, method and other parameters
    print(request_body)
    method = request_body["data"]["method"]
    device = request_body["device"]
    req_id = request_body["data"]["id"]
    # dependently of request method we send different data back
    if method == 'getCPULoad':
        gw.respond(device, req_id, {"CPU load": psutil.cpu_percent()})
    elif method == 'getMemoryLoad':
        gw.respond(device, req_id, {"Memory": psutil.virtual_memory().percent})
    else:
        print('Unknown method: ' + method)


gw = tb.TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
# now rpc_request_response will process rpc requests from servers
gw.set_server_side_rpc_request_handler(rpc_request_response)
# without device connection it is impossible to get any messages
gw.connect_device("Test Device A2")
gw.connect()
while True:
    time.sleep(1)
