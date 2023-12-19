import binance
import json
from binance.spot import Spot as Client
from datetime import datetime, timedelta
from binance_algorithmic_trading.logger import Logger


class ClientManager():
    '''
    A client manager class for working with the Binance Spot Web API Client.

    The client manager class implements all required functions for the
    binance-algo-trading application to GET and POST data from the Binance API.

    For all functionality, exception handling is implemented to ensure correct
    operation as well as to ensure the Binance server is not spammed with API
    requests which would result in the user IP from being banned.

    Future work should implement the ability to manage multiple Binance Client
    instances to execute orders for multiple accounts.

    Attributes
    ----------
    None

    Methods
    -------
    __init__(log_level, config)
        Initialise the client manager and Binance Spot API client
    get_exchange_info(symbols, permissions)
        Request exchange info for list of symbols and handle the response data
    get_depth(symbol, limit)
        Request depth for a given symbol
    get_klines(symbol, interval, start_time, end_time, limit)
        Request klines for a given symbol and interval between two times
    '''

    def __init__(self, log_level, config):
        '''
        Initialise the client manager and Binance Spot API client.

        Parameters
        ----------
        log_level: int
            The logging level as specified in the logging module
        config: Dict
            Dictionary containing the configuration settings for the Binance
            client connection, including API key and secret
        '''
        self.config = config
        self.exchange_info = None
        self.symbols = []
        self._x_mbx_used_weight = 0
        self._x_mbx_used_weight_1m = 0
        self._timeout = 5
        self._show_limit_usage = True
        self._local_time_zone = None
        self._client_error_retry_wait_time_s = 0
        self._client_error_restart_time = 0

        # Initialise logger for client manager
        self._logger = Logger(logger_name=__file__,
                              log_level=log_level).get_logger()

        try:
            # Cast config string values to required types
            timeout = int(config['TIMEOUT'])
            show_limit_usage = bool(config['SHOW_LIMIT_USAGE'] == 'TRUE')
            symbols = json.loads(config["SYMBOLS"])
        except Exception as e:
            self._logger.warning("invalid optional binance config value(s) in "
                                 "app.cfg, default values will be used, "
                                 f"error: {e}")
        else:
            self._timeout = timeout
            self._show_limit_usage = show_limit_usage
            self.symbols = symbols
            self._logger.debug("loaded config options for binance client")

        # Getting local timezone info
        self._local_time_zone = datetime.now().astimezone().tzinfo

        # Initialise binance API client
        # API key/secret are required for user data endpoints
        try:
            self.client = Client(
                base_url=self.config['BASE_URL'],
                api_key=self.config['API_KEY'],
                api_secret=self.config['SECRET_KEY'],
                timeout=self._timeout,
                show_limit_usage=self._show_limit_usage
            )
        except Exception as e:
            self._logger.critical(f"failed to initialise binance client: {e}")
            self._logger.critical("exiting application due to error")
            exit()
        else:
            self._logger.info("connected to binance")

    def get_exchange_info(self):

        if self._can_make_request():
            try:
                # Get exchange info for given symbols and permissions, if any
                result = self.client.exchange_info(symbols=self.symbols)
            except binance.error.ClientError as e:
                self._handle_client_error(e)
            else:
                self._logger.info("retrieved binance exchange information")
                # Update limit weights
                if 'limit_usage' in result:
                    self._update_weights(result['limit_usage'])
                if 'data' in result:
                    self.exchange_info = result['data']
        else:
            self._logger.warning("server request limit reached, need to wait "
                                f"until {self._client_error_restart_time}, "
                                "skipping action")

    def get_depth(self, symbol, limit=100):

        if self._can_make_request():
            self._logger.debug("requesting depth from binance")
            try:
                # Get order book for given symbol
                result = self.client.depth(symbol=symbol, limit=limit)
            except binance.error.ClientError as e:
                self._handle_client_error(e)
                return None
            else:
                # Update limit weights
                if 'limit_usage' in result:
                    self._update_weights(result['limit_usage'])

                # Return depth
                if 'data' in result: 
                    return result['data']
                else:
                    return None

    def get_klines(self, symbol, interval, start_time, end_time, limit):

        if self._can_make_request():
            self._logger.debug(f"requesting klines for '{symbol} {interval}' "
                               "between "
                               f"{datetime.fromtimestamp(start_time/1000)} "
                               f"and {datetime.fromtimestamp(end_time/1000)}")
            
            try:
                result = self.client.klines(
                    symbol,
                    interval,
                    startTime=start_time,
                    endTime=end_time,
                    limit=limit
                )
            except binance.error.ClientError as e:
                self._handle_client_error(e)
                return None
            else:
                # Update limit weights
                if 'limit_usage' in result:
                    self._update_weights(result['limit_usage'])

                # Return klines
                if 'data' in result:
                    return result['data']
                else:
                    return None

    def _update_weights(self, limit_usage):
        try:
            self._x_mbx_used_weight = limit_usage['x-mbx-used-weight']
            self._x_mbx_used_weight_1m = limit_usage['x-mbx-used-weight-1m']
        except KeyError:
            self._logger.warning("missing limit usage data in Binance response")
        else:
            self._logger.debug(f"client weights:\t{self._x_mbx_used_weight}\t"
                              f"{self._x_mbx_used_weight_1m}")

    def _can_make_request(self):

        # Check if a wait time has been set due to a client error
        if self._client_error_retry_wait_time_s > 0:

            # Check if current time has passed the wait time
            if (datetime.now() > self._client_error_restart_time):

                # Reset the wait time
                self._client_error_retry_wait_time_s = 0
                return True
            else:
                return False
        else:
            return True

    def _handle_client_error(self, e):

        # Parse error information
        self._client_error_http_status_code = e.status_code
        self._client_error_code = e.error_code
        self._client_error_message = e.error_message
        self._client_error_header = e.header
        self._client_error_error_data = e.error_data

        self._logger.error(f"binance client error: {e.error_message}")

        # Check what error code occurred
        match self._client_error_code:
            # Disconnected
            case -1001:
                pass
            # Unauthorized
            case -1002:
                pass
            # Too many requests from client
            case -1003:
                self._handle_client_error_1003()
            # Server busy
            case -1004:
                pass
            # Timeout
            case -1007:
                pass
            # Spot server overloaded
            case -1008:
                pass
            case _:
                pass

    def _handle_client_error_1003(self):

        # Get seconds to wait before retrying
        self._client_error_retry_wait_time_s = (
            int(self._client_error_header['retry-after'])
        )

        # Get time at which next request can be made
        self._client_error_restart_time = (
            datetime.now()
            + timedelta(
                seconds=self._client_error_retry_wait_time_s
            )
        )

        # Get weight limit usage
        limit_usage = {
            'x-mbx-used-weight': (
                self._client_error_header['x-mbx-used-weight']),
            'x-mbx-used-weight-1m': (
                self._client_error_header['x-mbx-used-weight-1m'])
        }
        self._update_weights(limit_usage)

        # Check what HTTP status code occurred
        match self._client_error_http_status_code:
            # IP banned
            case 418:
                self._logger.warning("client made too many requests, IP banned "
                                    f"until {self._client_error_restart_time}")
            # Too many requests, warning before IP ban
            case 429:
                self._logger.warning("client made too many requests, waiting "
                                    f"until {self._client_error_restart_time}"
                                    "s to avoid IP ban")
