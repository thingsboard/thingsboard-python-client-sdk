# ThingsBoard Python Client SDK 2.0

[![Join the chat at https://gitter.im/thingsboard/chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/thingsboard/chat?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<a href="https://thingsboard.io"><img src="./logo.png?raw=true" width="100" height="100"></a>

ThingsBoard is an open-source IoT platform for data collection, processing, visualization, and device management.  
This is the **official Python client SDK** for connecting **Devices** and **Gateways** to ThingsBoard using **MQTT**.

---

## ✨ Features

- **MQTT 3.1.1 / 5.0** with QoS 0 and 1
- **Async I/O** built on [`gmqtt`](https://github.com/wialon/gmqtt) for high performance
- **Encrypted connections** (TLS v1.2+)
- **Automatic reconnect** with session persistence
- **All Device MQTT APIs** ([docs](https://thingsboard.io/docs/reference/mqtt-api/))
- **All Gateway MQTT APIs** ([docs](https://thingsboard.io/docs/reference/gateway-mqtt-api/))
- **Batching** and **payload splitting** for large telemetry/attribute messages
- **Rate limit awareness** (ThingsBoard-style multi-window limits)
- **Delivery tracking** — know exactly when your message reaches the server
- **Device claiming**
- **Firmware updates**

---

## 📦 Installation

```bash
pip install tb-mqtt-client
```

## 🚀 Getting Started

This SDK is **async-based**.  
You can run it with `asyncio`.

We provide **ready-to-run examples** for both device and gateway clients.

---

### **Device Examples**  
[`examples/device/`](examples/device):
- [Claim to customer](examples/device/claim_device.py)
- [Provision and connect with provisioned credentials](examples/device/client_provisioning.py)
- [Retrieve firmware](examples/device/firmware_update.py)
- [Handle attribute updates from server](examples/device/handle_attribute_updates.py)
- [Handle RPC requests from server](examples/device/handle_rpc_requests.py)
- [Load testing](examples/device/load.py)
- [Operational example](examples/device/operational_example.py)
- [Request attributes from server](examples/device/request_attributes.py)
- [Send attributes to server](examples/device/send_attributes.py)
- [Send client-side RPC to server and retrieve the response](examples/device/send_client_side_rpc.py)
- [Send timeseries to server](examples/device/send_timeseries.py)
- [Connect to server using MQTT over SSL and send encrypted timeseries](examples/device/tls_connect.py)

---

### **Gateway Examples**  
[`examples/gateway/`](examples/gateway):
- [Claim device](examples/gateway/claim_device.py)
- [Connect and disconnect device](examples/gateway/connect_and_disconnect_device.py)
- [Handle attribute updates for connected devices](examples/gateway/handle_attribute_updates.py)
- [Handle RPC requests for connected devices](examples/gateway/handle_rpc_requests.py)
- [Load testing](examples/gateway/load.py)
- [Operational example](examples/gateway/operational_example.py)
- [Request attributes for connected devices](examples/gateway/request_attributes.py)
- [Send attributes for connected devices to server](examples/gateway/send_attributes.py)
- [Send timeseries for connected devices to server](examples/gateway/send_timeseries.py)
- [Connect gateway client to server using MQTT over SSL and send encrypted timeseries](examples/gateway/tls_connect.py)

---

## 📚 Documentation
- [ThingsBoard Device MQTT API](https://thingsboard.io/docs/reference/mqtt-api/)
- [ThingsBoard Gateway MQTT API](https://thingsboard.io/docs/reference/gateway-mqtt-api/)

---

## 💬 Support
- [Community chat](https://gitter.im/thingsboard/chat)
- [Q&A forum](https://groups.google.com/forum/#!forum/thingsboard)
- [Stack Overflow](http://stackoverflow.com/questions/tagged/thingsboard)

## 📄 Licenses

This project is released under [Apache 2.0 License](./LICENSE).
