"""Thingsboard HTTP API Device module"""

import requests


class TBHTTPClient:
    """Thingsboard HTTP API Device"""

    def __init__(self, host: str, token: str):
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        self.api_base_url = f'{host}/api/v1/{token}/'

    def connect(self):
        """Publish an empty telemetry data to ThingsBoard to test the connection."""
        self.publish_data({}, 'telemetry')

    def publish_data(self, data: dict, endpoint: str):
        """Send data to the ThingsBoard HTTP API."""
        response = self.session.post(f'{self.api_base_url}/{endpoint}', json=data)
        response.raise_for_status()

    def send_telemetry(self, telemetry: dict):
        """Publish telemetry to the ThingsBoard HTTP Device API."""
        self.publish_data(telemetry, 'telemetry')
