# ThingsBoard MQTT client Python SDK
[![Join the chat at https://gitter.im/thingsboard/chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/thingsboard/chat?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<img src="./logo.png?raw=true" width="100" height="100">

ThingsBoard is an open-source IoT platform for data collection, processing, visualization, and device management.
This project ia a Python library that provides convenient client SDK for both [Device](https://thingsboard.io/docs/reference/mqtt-api/) and [Gateway](https://thingsboard.io/docs/reference/gateway-mqtt-api/) APIs.

Device SDK supports:
- unencrypted and TLS connection;
- telemetry and attributes publishing;
- subscription to attributes;
- request attributes;
- respond to rpc calls;
- send client rpc calls;

Gateway SDK supports:
- unencrypted and TLS connection;
- telemetry and attributes publishing;
- subscription to attributes;
- request attributes;
- respond to rpc calls;

## Getting Started

To install using pip:
```
pip3 install tb-mqtt-client
```

Device connecting and telemetry publishing
```
from tb_device_mqtt import TBClient
telemetry = {"temperature": 41.9, "enabled": False, "currentFirmwareVersion": "v1.2.2"}
client = TBClient("127.0.0.1", "A1_TEST_TOKEN")
client.connect()
client.send_telemetry(telemetry)
client.disconnect()
```

TLS connection to localhost
```
from tb_device_mqtt import TBClient
import socket
client = TBClient(socket.gethostname())
client.connect(tls=True,
               ca_certs="mqttserver.pub.pem",
               cert_file="mqttclient.nopass.pem")
client.disconnect()
```
Subscription to attributes
```
import time
from tb_device_mqtt import TBClient

def callback(result):
    print(result)

client = TBClient("127.0.0.1", "A1_TEST_TOKEN")
client.connect()
client.subscribe("temperature", callback)
while True:
    time.sleep(1)
```

Gateway device connecting and disconnecting
```
from tb_gateway_mqtt import TBGateway
gateway = TBGateway("127.0.0.1", "SGxDCjGxUUnm5ZJOnYHh")
gateway.connect()
gateway.connect_device("Example Name")
gateway.disconnect_device("Example Name")
gateway.disconnect()
```
There are more examples for both [device](https://github.com/serhiilikh/tb_mqtt_client/tree/master/examples/device) and [gateway](https://github.com/serhiilikh/tb_mqtt_client/tree/master/examples/gateway) in corresponding folders.
сюда примеры не из моей папки, а из будущей папки примеров tb!

## Support

 - [Community chat](https://gitter.im/thingsboard/chat)
 - [Q&A forum](https://groups.google.com/forum/#!forum/thingsboard)
 - [Stackoverflow](http://stackoverflow.com/questions/tagged/thingsboard)

## Licenses

This project is released under [Apache 2.0 License](./LICENSE).
