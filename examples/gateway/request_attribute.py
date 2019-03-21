import logging
import tb_gateway_mqtt as tb
logging.basicConfig(level=logging.DEBUG)


def callback(result):
    print(result)


client = tb.TBGateway("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")
client.connect()
client.request_attributes(["temp"], callback=callback)
while True:
    pass













# Topic: v1/gateway/attributes/request
# Message: {"id": $request_id, "device": "Device A", "client": true, "key": "attribute1"}