import tb_mqtt_client as tb
import json
import logging

#logging.basicConfig(level=logging.INFO)
telemetry = {"temp": 1001}
client = tb.TbClient("demo.thingsboard.io", "v5cgxxXGHvuFwdxENEc7")


client.connect()
import time
# t = time.time()
# for x in range(1000):
#     pass
#     #client.send_telemetry(telemetry)
# print("lite",time.time() - t)
#

t = time.time()
for x in range(1000):
    pass
    #client.send_telemetry(telemetry,quality_of_service=1)
print("lite + qos=1",time.time() - t)

#
# t = time.time()
# for x in range(100):
#     client.send_telemetry(telemetry,blocking=1)
# print("blocking",time.time() - t)


import threading

# t = time.time()
# for x in range(100):
#     client.send_telemetry(telemetry,blocking=1,quality_of_service=1)
# print("blocking+qos1",time.time() - t)


tim = time.time()
for x in range(1000):
    #t = threading.Thread(target=client.send_telemetry,args=[telemetry,blocking=1,quality_of_service=1])
    t = threading.Thread(target=client.send_telemetry, args=(telemetry, 0, 0))
    t.start()
print("blocking+qos1",time.time() - tim)



client.disconnect()


# lite 0.04897356033325195
# lite + qos=1 0.008728742599487305
# blocking 0.23769092559814453
# blocking+qos1 17.027390956878662