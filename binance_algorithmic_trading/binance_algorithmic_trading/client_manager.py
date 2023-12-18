from binance_algorithmic_trading.logger import Logger
from binance.spot import Spot as Client
import binance
from math import floor
from datetime import datetime, timedelta

class ClientManager():

    x_mbx_used_weight = 0
    x_mbx_used_weight_1m = 0
    client_error_retry_wait_time_s = 0
    client_error_restart_time = 0
    timeout = 5
    show_limit_usage = True
    initialised = False
    tz = None

    weights = {
        'EXCHANGE_INFO': 20
    }

    request_types = [
        "REQUEST_WEIGHT",
        "ORDERS",
        "RAW_REQUESTS"
    ]

    def __init__(self, log_level, config):

        # Initialise logger for client manager
        self.logger = Logger(logger_name=__file__,
                             log_level=log_level).get_logger()

        self.config = config
        self.base_url = config['BASE_URL']
        self.api_key = config['API_KEY']
        self.api_secret = config['SECRET_KEY']

        try:
            # Cast config string values to required types
            timeout = int(config['TIMEOUT'])
            show_limit_usage = bool(config['SHOW_LIMIT_USAGE'] == 'TRUE')
        except Exception as e:
            self.logger.warning("invalid optional binance config value(s) in "
                                "app.cfg, default values will be used, error: "
                                f"{e}")
        else:
            self.timeout = timeout
            self.show_limit_usage = show_limit_usage
            self.logger.debug("loaded config options for binance client")

        # Getting local timezone info
        self.tz = datetime.now().astimezone().tzinfo

        # Initialise binance API client
        # API key/secret are required for user data endpoints
        try:
            self.client = Client(
                base_url=self.base_url,
                api_key=self.api_key,
                api_secret=self.api_secret,
                timeout=self.timeout,
                show_limit_usage=self.show_limit_usage
            )
        except Exception as e:
            self.logger.exception(f"failed to initialise binance client: {e}")
            self.logger.info("exiting application due to error")
            exit()
        else:
            self.initialised = True
            self.logger.debug("initialised binance client")            

    
    def get_exchange_info(self, symbols=None, permissions=None):

        if self._can_make_request():
            try:
                # Get exchange info for given symbols and permissions, if any
                result = self.client.exchange_info(symbols, permissions)
                
                # Update limit weights
                if 'limit_usage' in result: self._update_weights(result['limit_usage']) 

                # Save the data
                data = result['data']
                self.timezone = data['timezone']
                self.server_time = data['serverTime']
                self.rate_limits = data['rateLimits']
                self.exchange_filters = data['exchangeFilters']
            except binance.error.ClientError as e:
                self._handle_client_error(e)
                return None
            else:
                # Update limit weights
                if 'limit_usage' in result: self._update_weights(result['limit_usage']) 
                if 'data' in result: return result['data']
        else:
            self.logger.warning(f"server request limit reached, need to wait until {self.client_error_restart_time}, skipping action")
            return None

    def get_depth(self, symbol, limit=100):

        if self._can_make_request():
            self.logger.debug(f"requesting depth from binance")
            try: 
                # Get order book for given symbol
                result = self.client.depth(symbol=symbol, limit=limit)
            except binance.error.ClientError as e:
                self._handle_client_error(e)
                return None
            else:
                # Update limit weights
                if 'limit_usage' in result: self._update_weights(result['limit_usage'])
                if 'data' in result: return result['data']
                else: return None

    def get_klines(self, symbol, interval, start_time, end_time, limit):

        if self._can_make_request():
            self.logger.debug(f"requesting klines for '{symbol} {interval}' between {start_time} and {end_time}") 
    
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
                if 'limit_usage' in result: self._update_weights(result['limit_usage'])

                # Return klines
                if 'data' in result: return result['data']
                else: return None


    def _update_weights(self, limit_usage):
            self.x_mbx_used_weight = limit_usage['x-mbx-used-weight']
            self.x_mbx_used_weight_1m = limit_usage['x-mbx-used-weight-1m']
            self.logger.debug(f"client weights:\t{self.x_mbx_used_weight}\t{self.x_mbx_used_weight_1m}")
    
    def _get_weights(self):
        return (self.x_mbx_used_weight, self.x_mbx_used_weight_1m)
    
    def _can_use_weight(self, weight, action_type):
        pass

    def _can_make_request(self):

        # Check if a wait time has been set due to a client error
        if self.client_error_retry_wait_time_s > 0:

            # Check if current time has passed the wait time
            if (datetime.now() > self.client_error_restart_time):

                # Reset the wait time
                self.client_error_retry_wait_time_s = 0
                return True
            else:
                return False
        else:
            return True 

    def _handle_client_error(self, e):

        # Parse error information
        self.client_error_http_status_code = e.status_code
        self.client_error_code = e.error_code
        self.client_error_message = e.error_message
        self.client_error_header = e.header
        self.client_error_error_data = e.error_data

        self.logger.error(f"binance client error: {e.error_message}")

        # Check what error code occurred
        match self.client_error_code:
            # Disconnected
            case -1001:
                pass
            # Unauthorized
            case -1002:
                pass
            # Too many requests
            case -1003:
                # Check what HTTP status code occurred
                match self.client_error_http_status_code:
                    # IP banned
                    case 418:
                        # Get time to wait before retrying
                        self.client_error_retry_wait_time_s = int(self.client_error_header['retry-after'])
                        self.client_error_restart_time = datetime.now() + timedelta(seconds=self.client_error_retry_wait_time_s)
                        limit_usage = {
                            'x-mbx-used-weight': self.client_error_header['x-mbx-used-weight'],
                            'x-mbx-used-weight-1m': self.client_error_header['x-mbx-used-weight-1m']
                        }
                        self._update_weights(limit_usage)
                        self.logger.warning(f"client made too many requests, IP banned until {self.client_error_restart_time}s")
                    # Too many requests, warning before IP ban
                    case 429:
                        # Get time to wait before retrying
                        self.client_error_retry_wait_time_s = int(self.client_error_header['retry-after'])
                        self.client_error_restart_time = datetime.now() + timedelta(seconds=self.client_error_retry_wait_time_s)
                        limit_usage = {
                            'x-mbx-used-weight': self.client_error_header['x-mbx-used-weight'],
                            'x-mbx-used-weight-1m': self.client_error_header['x-mbx-used-weight-1m']
                        }
                        self._update_weights(limit_usage)
                        self.logger.warning(f"client made too many requests, waiting until {self.client_error_restart_time}s to avoid IP ban")
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