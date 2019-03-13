import logging, logging.handlers
import os.path
import tb_mqtt_client as tb
logging.basicConfig(level=logging.INFO)

def callback(result):
    print(result)

def on_server_side_rpc_request (requestId, requestBody):
    client.on_server_side_rpc_response(requestId, {"ololo":"trololo"})

client = tb.TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
client.connect()
client.set_serever_side_rpc_request_handler(on_server_side_rpc_request)

sub_id = client.subscribe(callback, "ololo")
client.unsubscribe(sub_id)


while True:
    pass
