"""Thingsboard HTTP API Device module"""
import threading
from datetime import datetime, timezone
import requests


class TBHTTPAPIException(Exception):
    """ThingsBoard HTTP Device API Exception class."""


class TBProvisionFailure(TBHTTPAPIException):
    """Exception raised if device provisioning failed."""


class TBHTTPClient:
    """Thingsboard HTTP API Device"""

    def __init__(self, host: str, token: str, name: str = None):
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self.token = token
        self.name = name
        self.host = host
        self.api_base_url = f'{self.host}/api/v1/{self.token}'
        self.subscriptions = {
            'attributes': {
                'event': threading.Event()
            },
            'rpc': {
                'event': threading.Event()
            }
        }

    def __repr__(self):
        return f'<ThingsBoard ({self.host}) HTTP client {self.name}>'

    def connect(self):
        """Publish an empty telemetry data to ThingsBoard to test the connection."""
        self.publish_data({}, 'telemetry')

    def publish_data(self, data: dict, endpoint: str) -> dict:
        """Send data to the ThingsBoard HTTP API."""
        response = self.session.post(f'{self.api_base_url}/{endpoint}', json=data)
        response.raise_for_status()
        return response.json() if response.content else {}

    def get_data(self, params: dict, endpoint: str) -> dict:
        """Send data to the ThingsBoard HTTP API."""
        response = self.session.get(f'{self.api_base_url}/{endpoint}', params=params)
        response.raise_for_status()
        return response.json()

    def send_telemetry(self, telemetry: dict, timestamp: datetime = None):
        """Publish telemetry to the ThingsBoard HTTP Device API."""
        if timestamp:
            # Convert timestamp to UTC milliseconds as required by API specification.
            payload = {
                'ts': int(timestamp.replace(tzinfo=timezone.utc).timestamp()*1000),
                'values': telemetry
            }
        else:
            payload = telemetry
        self.publish_data(payload, 'telemetry')

    def send_attributes(self, attributes: dict):
        """Send attributes to the ThingsBoard HTTP Device API."""
        self.publish_data(attributes, 'attributes')

    def send_rpc(self, name: str, params: dict = None) -> dict:
        """Send RPC to the ThingsBoard HTTP Device API."""
        return self.publish_data({'method': name, 'params': params or {}}, 'rpc')

    def request_attributes(self, client_keys: list = None, shared_keys: list = None) -> dict:
        """Request attributes from the ThingsBoard HTTP Device API."""
        params = {'client_keys': client_keys, 'shared_keys': shared_keys}
        return self.get_data(params=params, endpoint='attributes')

    def subscribe_to_attributes(self, callback, timeout: int = None):
        """Subscribe to shared attributes updates from the ThingsBoard HTTP device API."""
        params = {'timeout': timeout} if timeout else {}

        def subscription():
            self.subscriptions['attributes']['event'].clear()
            while True:
                response = self.session.get(url=f'{self.api_base_url}/attributes/updates',
                                            params=params)
                if self.subscriptions['attributes']['event'].is_set():
                    break
                if response.status_code == 408 and timeout:
                    break
                if response.status_code == 504:  # Gateway Timeout
                    continue  # Reconnect
                response.raise_for_status()
                callback(response.json())
            self.subscriptions['attributes']['event'].clear()

        self.subscriptions['attributes']['thread'] = threading.Thread(
            name='subscribe_attributes',
            target=subscription,
            daemon=True)
        self.subscriptions['attributes']['thread'].start()

    def unsubscribe_from_attributes(self):
        """Unsubscribe shared attributes updates from the ThingsBoard HTTP device API."""
        self.subscriptions['attributes']['event'].set()

    def subscribe_to_rpc(self, callback, timeout: int = None):
        """Subscribe to RPC from the ThingsBoard HTTP device API."""
        params = {'timeout': timeout} if timeout else {}

        def subscription():
            self.subscriptions['rpc']['event'].clear()
            while True:
                response = self.session.get(url=f'{self.api_base_url}/rpc',
                                            params=params)
                if self.subscriptions['rpc']['event'].is_set():
                    break
                if response.status_code == 408 and timeout:
                    break
                if response.status_code == 504:  # Gateway Timeout
                    continue  # Reconnect
                response.raise_for_status()
                callback(response.json())
            self.subscriptions['rpc']['event'].clear()

        self.subscriptions['rpc']['thread'] = threading.Thread(
            name='subscribe_rpc',
            target=subscription,
            daemon=True)
        self.subscriptions['rpc']['thread'].start()

    def unsubscribe_from_rpc(self):
        """Unsubscribe to RPC from the ThingsBoard HTTP device API."""
        self.subscriptions['rpc']['event'].set()

    @classmethod
    def provision(cls, host: str, device_name: str, device_key: str, device_secret: str):
        """Initiate device provisioning through the ThingsBoard HTTP Device API."""
        data = {
            'deviceName': device_name,
            'provisionDeviceKey': device_key,
            'provisionDeviceSecret': device_secret
        }
        response = requests.post(f'{host}/api/v1/provision', json=data)
        response.raise_for_status()
        device = response.json()
        if device['status'] == 'SUCCESS' and device['credentialsType'] == 'ACCESS_TOKEN':
            return cls(host=host, token=device['credentialsValue'], name=device_name)
        raise TBProvisionFailure(device)
