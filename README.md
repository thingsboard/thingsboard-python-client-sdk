# TThingsBoard MQTT client Python SDK
[![Join the chat at https://gitter.im/thingsboard/chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/thingsboard/chat?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

ThingsBoard is an open-source IoT platform for data collection, processing, visualization, and device management.
This project ia a Python library that provides convenient client SDK for both [Device](https://thingsboard.io/docs/reference/mqtt-api/) and [Gateway](https://thingsboard.io/docs/reference/gateway-mqtt-api/) APIs.

<img src="./img/logo.png?raw=true" width="100" height="100">

## Documentation

ThingsBoard documentation is hosted on [thingsboard.io](https://thingsboard.io/docs).

## Getting Started

pip3 install tb-mqtt-client

from tb_device_mqtt import TBClient
telemetry = {"temperature": 41.9, "enabled": False, "currentFirmwareVersion": "v1.2.2"}
client = TBClient("127.0.0.1", "A2_TEST_TOKEN")
client.connect()
client.send_telemetry(telemetry)
client.disconnect()

## Support

 - [Community chat](https://gitter.im/thingsboard/chat)
 - [Q&A forum](https://groups.google.com/forum/#!forum/thingsboard)
 - [Stackoverflow](http://stackoverflow.com/questions/tagged/thingsboard)

## Licenses

This project is released under [Apache 2.0 License](./LICENSE).
