import time
import logging
from tb_device_mqtt import TBDeviceMqttClient
logging.basicConfig(level=logging.DEBUG)


def callback(client, request_id, resp_body, exception):
    client.stop()
    if exception is not None:
        print("Exception: " + str(exception))
    else:
        print("request id: {request_id}, response body: {resp_body}".format(request_id=request_id,
                                                                            resp_body=resp_body))


def main():
    client = TBDeviceMqttClient("127.0.0.1", "A2_TEST_TOKEN")

    client.connect()
    # call "getTime" on server and receive result, then process it with callback
    client.send_rpc_call("getTime", {}, callback)
    while not client.stopped:
        time.sleep(1)


if __name__ == '__main__':
    main()
