import logging
from tb_device_mqtt import TBClient
import socket

logging.basicConfig(level=logging.DEBUG)
client = TBClient(socket.gethostname())
client.connect(tls=True,
               ca_certs="mqttserver.pub.pem",
               cert_file="mqttclient.nopass.pem")
while True:
    pass
