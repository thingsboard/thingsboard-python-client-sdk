"""ThingsBoard HTTP API device module."""
import threading
import logging
import queue
import time
import typing
from datetime import datetime, timezone
import requests


class TBHTTPAPIException(Exception):
    """ThingsBoard HTTP Device API Exception class."""


class TBProvisionFailure(TBHTTPAPIException):
    """Exception raised if device provisioning failed."""


class TBHTTPDevice:
    """ThingsBoard HTTP Device API class.

    :param host: The ThingsBoard hostname.
    :param token: The device token.
    :param name: A name for this device. The name is only set locally.
    """

    def __init__(self, host: str, token: str, name: str = None):
        self.__session = requests.Session()
        self.__session.headers.update({'Content-Type': 'application/json'})
        self.__config = {
            'host': host, 'token': token, 'name': name, 'timeout': 30
        }
        self.__worker = {
            'publish': {
                'queue': queue.Queue(),
                'thread': threading.Thread(target=self.__publish_worker, daemon=True),
                'stop_event': threading.Event()
            },
            'attributes': {
                'thread': threading.Thread(target=self.__subscription_worker,
                                           daemon=True,
                                           kwargs={'endpoint': 'attributes'}),
                'stop_event': threading.Event(),
            },
            'rpc': {
                'thread': threading.Thread(target=self.__subscription_worker,
                                           daemon=True,
                                           kwargs={'endpoint': 'rpc'}),
                'stop_event': threading.Event(),
            }
        }

    def __repr__(self):
        return f'<ThingsBoard ({self.host}) HTTP device {self.name}>'

    @property
    def host(self) -> str:
        """Get the ThingsBoard hostname."""
        return self.__config['host']

    @property
    def name(self) -> str:
        """Get the device name."""
        return self.__config['name']

    @property
    def timeout(self) -> int:
        """Get the connection timeout."""
        return self.__config['timeout']

    @property
    def api_base_url(self) -> str:
        """Get the ThingsBoard API base URL."""
        return f'{self.host}/api/v1/{self.token}'

    @property
    def token(self) -> str:
        """Get the device token."""
        return self.__config['token']

    @property
    def logger(self) -> logging.Logger:
        """Get the logger instance."""
        return logging.getLogger('TBHTTPDevice')

    @property
    def log_level(self) -> str:
        """Get the log level."""
        levels = {0: 'NOTSET', 10: 'DEBUG', 20: 'INFO', 30: 'WARNING', 40: 'ERROR', 50: 'CRITICAL'}
        return levels.get(self.logger.level)

    @log_level.setter
    def log_level(self, value: typing.Union[int, str]):
        self.logger.setLevel(value)
        self.logger.critical('Log level set to %s', self.log_level)

    def start_publish_worker(self):
        """Start the publish worker thread."""
        self.__worker['publish']['stop_event'].clear()
        self.__worker['publish']['thread'].start()

    def stop_publish_worker(self):
        """Stop the publish worker thread."""
        self.__worker['publish']['stop_event'].set()

    def __publish_worker(self):
        """Publish telemetry data from the queue."""
        logger = self.logger.getChild('worker.publish')
        logger.info('Start publisher thread')
        logger.debug('Perform connection test before entering worker loop')
        if not self.test_connection():
            logger.error('Connection test failed, exit publisher thread')
            return
        logger.debug('Connection test successful')
        while True:
            try:
                task = self.__worker['publish']['queue'].get(timeout=1)
            except queue.Empty:
                if self.__worker['publish']['stop_event'].is_set():
                    break
                continue
            endpoint = task.pop('endpoint')
            try:
                self._publish_data(task, endpoint)
            except Exception as error:
                # ToDo: More precise exception catching
                logger.error(error)
                task.update({'endpoint': endpoint})
                self.__worker['publish']['queue'].put(task)
                time.sleep(1)
            else:
                logger.debug('Published %s to %s', task, endpoint)
                self.__worker['publish']['queue'].task_done()
        logger.info('Stop publisher thread.')

    def test_connection(self) -> bool:
        """Test connection to the API.

        :return: True if no errors occurred, False otherwise.
        """
        self.logger.debug('Start connection test')
        success = False
        try:
            self._publish_data(data={}, endpoint='telemetry')
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as error:
            self.logger.debug(error)
        except requests.exceptions.HTTPError as error:
            self.logger.debug(error)
            status_code = error.response.status_code
            if status_code == 401:
                self.logger.error('Error 401: Unauthorized. Check if token is correct.')
            else:
                self.logger.error('Error %s', status_code)
        else:
            self.logger.debug('Connection test successful')
            success = True
        finally:
            self.logger.debug('End connection test')
        return success

    def connect(self) -> bool:
        """Publish an empty telemetry data to ThingsBoard to test the connection.

        :return: True if connected, false otherwise.
        """
        if self.test_connection():
            self.logger.info('Connected to ThingsBoard')
            self.start_publish_worker()
            return True
        return False

    def _publish_data(self, data: dict, endpoint: str, timeout: int = None) -> dict:
        """Send POST data to ThingsBoard.

        :param data: The data dictionary to send.
        :param endpoint: The receiving API endpoint.
        :param timeout: Override the instance timeout for this request.
        """
        response = self.__session.post(
            url=f'{self.api_base_url}/{endpoint}',
            json=data,
            timeout=timeout or self.timeout)
        response.raise_for_status()
        return response.json() if response.content else {}

    def _get_data(self, params: dict, endpoint: str, timeout: int = None) -> dict:
        """Retrieve data with GET from ThingsBoard.

        :param params: A dictionary with the parameters for the request.
        :param endpoint: The receiving API endpoint.
        :param timeout: Override the instance timeout for this request.
        :return: A dictionary with the response from the ThingsBoard instance.
        """
        response = self.__session.get(
            url=f'{self.api_base_url}/{endpoint}',
            params=params,
            timeout=timeout or self.timeout)
        response.raise_for_status()
        return response.json()

    def send_telemetry(self, telemetry: dict, timestamp: datetime = None, queued: bool = True):
        """Publish telemetry to ThingsBoard.

        :param telemetry: A dictionary with the telemetry data to send.
        :param timestamp: Timestamp to set for the values. If not set the ThingsBoard server uses
            the time of reception as timestamp.
        :param queued: Add the telemetry to the queue. If False, the data is send immediately.
        """
        timestamp = datetime.now() if timestamp is None else timestamp
        payload = {
            'ts': int(timestamp.replace(tzinfo=timezone.utc).timestamp()*1000),
            'values': telemetry,
        }
        if queued:
            payload.update({'endpoint': 'telemetry'})
            self.__worker['publish']['queue'].put(payload)
        else:
            self._publish_data(payload, 'telemetry')

    def send_attributes(self, attributes: dict):
        """Send attributes to ThingsBoard.

        :param attributes: Attributes to send.
        """
        self._publish_data(attributes, 'attributes')

    def send_rpc(self, name: str, params: dict = None, rpc_id: int = None) -> dict:
        """Send RPC to ThingsBoard and return response.

        :param name: Name of the RPC method.
        :param params: Parameter for the RPC.
        :param rpc_id: Specify an Id for this RPC.
        :return: A dictionary with the response.
        """
        endpoint = f'rpc/{rpc_id}' if rpc_id else 'rpc'
        return self._publish_data({'method': name, 'params': params or {}}, endpoint)

    def request_attributes(self, client_keys: list = None, shared_keys: list = None) -> dict:
        """Request attributes from ThingsBoard.

        :param client_keys: A list of keys for client attributes.
        :param shared_keys: A list of keys for shared attributes.
        :return: A dictionary with the request attributes.
        """
        params = {'client_keys': client_keys, 'shared_keys': shared_keys}
        return self._get_data(params=params, endpoint='attributes')

    def __subscription_worker(self, endpoint: str, timeout: int = None):
        """Worker thread for subscription to HTTP API endpoints.

        :param endpoint: The endpoint name.
        :param timeout: Timeout value in seconds.
        """
        logger = self.logger.getChild(f'worker.subscription.{endpoint}')
        stop_event = self.__worker[endpoint]['stop_event']
        logger.info('Start subscription to %s updates', endpoint)

        if not self.__worker[endpoint].get('callback'):
            logger.warning('No callback set for %s subscription', endpoint)
            stop_event.set()
        callback = self.__worker[endpoint].get('callback', lambda data: None)
        params = {
            'timeout': (timeout or self.timeout)*1000
        }
        url = {
            'attributes': f'{self.api_base_url}/attributes/updates',
            'rpc': f'{self.api_base_url}/rpc'
        }
        logger.debug('Timeout set to %ss', params['timeout']/1000)
        while not stop_event.is_set():
            response = self.__session.get(url=url[endpoint],
                                          params=params,
                                          timeout=params['timeout'])
            if stop_event.is_set():
                break
            if response.status_code == 408:  # Request timeout
                continue
            if response.status_code == 504:  # Gateway Timeout
                continue  # Reconnect
            response.raise_for_status()
            callback(response.json())
        stop_event.clear()
        logger.info('Stop subscription to %s updates', endpoint)

    def subscribe(self, endpoint: str, callback: typing.Callable[[dict], None] = None):
        """Subscribe to updates from a given endpoint.

        :param endpoint: The endpoint to subscribe.
        :param callback: Callback to execute on an update. Takes a dict as only argument.
        """
        if endpoint not in ['attributes', 'rpc']:
            raise ValueError
        if callback:
            if not callable(callback):
                raise TypeError
            self.__worker[endpoint]['callback'] = callback
        self.__worker[endpoint]['stop_event'].clear()
        self.__worker[endpoint]['thread'].start()

    def unsubscribe(self, endpoint: str):
        """Unsubscribe from a given endpoint.

        :param endpoint: The endpoint to unsubscribe.
        """
        if endpoint not in ['attributes', 'rpc']:
            raise ValueError
        self.logger.debug('Set stop event for %s subscription', endpoint)
        self.__worker[endpoint]['stop_event'].set()

    @classmethod
    def provision(cls, host: str, device_name: str, device_key: str, device_secret: str):
        """Initiate device provisioning and return a device instance.

        :param host: The root URL to the ThingsBoard instance.
        :param device_name: Name of the device to provision.
        :param device_key: Provisioning device key from ThingsBoard.
        :param device_secret: Provisioning secret from ThingsBoard.
        :return: Instance of :class:`TBHTTPClient`
        """
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


class TBHTTPClient(TBHTTPDevice):
    """Legacy class name."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger.critical('TBHTTPClient class is deprecated, please use TBHTTPDevice')
