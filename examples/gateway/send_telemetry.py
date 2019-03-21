import logging
from tb_gateway_mqtt import TBGateway
logging.basicConfig(level=logging.INFO)
import time


gateway = TBGateway("demo.thingsboard.io", "VSBk9a8nrkiGrMdDUEmm")
telemetry = {
    "Example Name": [
        {
            "ts": 1483228800000,
            "values": {
                "temperature": 42,
                "humidity": 98
            }
        },
        {
            "ts": int(time.time()),
            "values": {
                "temperature": 43,
                "humidity": 98
            }
        }
    ],
    "Test Device A1": [
        {
            "ts": 1483228800000,
            "values": {
                "temperature": 42,
                "humidity": 80
            }
        }
    ]
}
#gateway.send_telemetry(telemetry)
while True:
    pass