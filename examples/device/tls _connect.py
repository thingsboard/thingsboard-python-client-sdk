import logging
from tb_device_mqtt import TBDeviceMqttClient
import socket

logging.basicConfig(level=logging.DEBUG)
# connecting to localhost
client = TBDeviceMqttClient(socket.gethostname())
client.connect(tls=True,
               ca_certs="mqttserver.pub.pem",
               cert_file="mqttclient.nopass.pem")
client.disconnect()
