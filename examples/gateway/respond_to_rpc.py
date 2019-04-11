import logging.handlers
import time

from tb_gateway_mqtt import TBGatewayMqttClient
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
        gateway.gw_send_rpc_reply(device, req_id, {"CPU load": psutil.cpu_percent()})
    elif method == 'getMemoryLoad':
        gateway.gw_send_rpc_reply(device, req_id, {"Memory": psutil.virtual_memory().percent})
    else:
        print('Unknown method: ' + method)


gateway = TBGatewayMqttClient("127.0.0.1", "TEST_GATEWAY_TOKEN")
gateway.connect()
# now rpc_request_response will process rpc requests from servers
gateway.gw_set_server_side_rpc_request_handler(rpc_request_response)
# without device connection it is impossible to get any messages
gateway.gw_connect_device("Test Device A2")
while True:
    time.sleep(1)
