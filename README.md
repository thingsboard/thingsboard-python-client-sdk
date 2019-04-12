# ThingsBoard MQTT client Python SDK
[![Join the chat at https://gitter.im/thingsboard/chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/thingsboard/chat?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<img src="./logo.png?raw=true" width="100" height="100">

ThingsBoard is an open-source IoT platform for data collection, processing, visualization, and device management.
This project ia a Python library that provides convenient client SDK for both [Device](https://thingsboard.io/docs/reference/mqtt-api/) 
and [Gateway](https://thingsboard.io/docs/reference/gateway-mqtt-api/) APIs.

SDK supports:
- Unencrypted and encrypted (TLS v1.2) connection;
- QoS 0 and 1;
- Automatic reconnect;
- All Device MQTT APIs provided by ThingsBoard
- All Gateway MQTT APIs provided by ThingsBoard

SDK is based on Paho MQTT library. 

## Installation

To install using pip:

```bash
pip3 install tb-mqtt-client
```

## Getting Started

Client initialization and telemetry publishing

```python
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
telemetry = {"temperature": 41.9, "enabled": False, "currentFirmwareVersion": "v1.2.2"}
client = TBDeviceMqttClient("127.0.0.1", "A1_TEST_TOKEN")
# Connect to ThingsBoard
client.connect()
# Sending telemetry without checking the delivery status
client.send_telemetry(telemetry) 
# Sending telemetry and checking the delivery status (QoS = 1 by default)
result = client.send_telemetry(telemetry)
# get is a blocking call that awaits delivery status  
success = result.get() == TBPublishInfo.TB_ERR_SUCCESS
# Disconnect from ThingsBoard
client.disconnect()
```

### Connection using TLS

TLS connection to localhost. See https://thingsboard.io/docs/user-guide/mqtt-over-ssl/ for more information about client and ThingsBoard configuration.

```python
from tb_device_mqtt import TBDeviceMqttClient
import socket
client = TBDeviceMqttClient(socket.gethostname())
client.connect(tls=True,
               ca_certs="mqttserver.pub.pem",
               cert_file="mqttclient.nopass.pem")
client.disconnect()
```

## Using Device APIs

### Subscription to attributes. 

```python
import time
from tb_device_mqtt import TBDeviceMqttClient

def callback(result):
    print(result)

client = TBDeviceMqttClient("127.0.0.1", "A1_TEST_TOKEN")
client.connect()
client.subscribe_to_attribute("uploadFrequency", callback)
client.subscribe_to_all_attributes(callback)
while True:
    time.sleep(1)
```

## Using Gateway APIs

**TBGatewayMqttClient** extends **TBDeviceMqttClient**, thus has access to all it's APIs as a regular device.
Besides, gateway is able to represent multiple devices connected to it. For example, sending telemetry or attributes on behalf of other, constrained, device. See more info about the gateway here: 
 
```python
import time
from tb_gateway_mqtt import TBGatewayMqttClient
gateway = TBGatewayMqttClient("127.0.0.1", "GATEWAY_TEST_TOKEN")
gateway.connect()
gateway.gw_connect_device("Device A1")

gateway.gw_send_telemetry("Test Device A2", {"ts": int(round(time.time() * 1000)), "values": {"temperature": 42.2}})
gateway.gw_send_attributes("Test Device A2", {"firmwareVersion": "2.3.1"})

gateway.gw_disconnect_device("Device A1")
gateway.disconnect()
```


## Other Examples

There are more examples for both [device](/tree/master/examples/device) and [gateway](/tree/master/examples/gateway) in corresponding [folders](/tree/master/examples/).

## Support

 - [Community chat](https://gitter.im/thingsboard/chat)
 - [Q&A forum](https://groups.google.com/forum/#!forum/thingsboard)
 - [Stackoverflow](http://stackoverflow.com/questions/tagged/thingsboard)

## Licenses

This project is released under [Apache 2.0 License](./LICENSE).
