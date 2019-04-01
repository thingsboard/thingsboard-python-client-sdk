import logging.handlers
import tb_gateway_mqtt as tb
import psutil
logging.basicConfig(level=logging.DEBUG)

gw = tb.TBGateway("demo.thingsboard.io", "VSBk9a8nrkiGrMdDUEmm")


def rpc_request_response(request_id, request_body):
    print(request_id, request_body)
    gw.respond(request_id, {"Memory": psutil.virtual_memory().percent})


gw.set_server_side_rpc_request_handler(rpc_request_response)
gw.connect()
while True:
    pass
