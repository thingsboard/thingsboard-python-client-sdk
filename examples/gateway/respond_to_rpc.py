import logging.handlers
import tb_gateway_mqtt as tb
import psutil
logging.basicConfig(level=logging.DEBUG)

gw = tb.TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")


def rpc_request_response(request_id, request_bode):
    gw.respond(request_id, {"Memory": psutil.virtual_memory().percent})


gw.set_server_side_rpc_request_handler(rpc_request_response)
gw.connect()


while True:
    pass
