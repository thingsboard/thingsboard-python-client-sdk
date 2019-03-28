import logging
from tb_gateway_mqtt import TBGateway
logging.basicConfig(level=logging.DEBUG)
import time

gateway = TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
telemetry = [
    {"ts": 1, "values": {"key1": "val1"}},
    {"ts": 2, "values": {"key2": "val2"}}
]
telemetry = {"ts": 2, "values": {"key3": "value3"}}
attributes = {"atr1": 1, "atr2": True, "atr3": "value3"}
gateway.connect()
gateway.send_attributes("Test Device A2", attributes)
gateway.send_telemetry("Test Device A2", telemetry)
while True:
    pass
