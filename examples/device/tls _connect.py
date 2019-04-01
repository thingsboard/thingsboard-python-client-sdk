import logging
from tb_device_mqtt import TBClient
import socket

logging.basicConfig(level=logging.DEBUG)
# connecting to localhost
client = TBClient(socket.gethostname())
client.connect(tls=True,
               ca_certs="mqttserver.pub.pem",
               cert_file="mqttclient.nopass.pem")
client.disconnect()
