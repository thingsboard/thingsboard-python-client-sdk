import logging
from tb_gateway_mqtt import TBGateway
import socket

logging.basicConfig(level=logging.DEBUG)
# connecting to localhost
gateway = TBGateway(socket.gethostname())
gateway.connect(tls=True,
                ca_certs="mqttserver.pub.pem",
                cert_file="mqttclient.nopass.pem")
gateway.disconnect()
